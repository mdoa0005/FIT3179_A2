import pandas as pd

# 1. Population Data (June 2024 ABS - full names for merge)
pop_data = {
    'State or territory': ['New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia', 'Tasmania', 'Northern Territory', 'Australian Capital Territory'],
    'Population': [8484400, 6981400, 5586300, 1878000, 2965200, 575400, 255100, 474100],
    'State': ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
}
pop_df = pd.DataFrame(pop_data)
pop_df.to_csv('population.csv', index=False)
print("Created population.csv")

# 2. Renewables GWh by State (from State summary 2023-24 sheet)
gwh_df = pd.read_excel('Australian Energy Statistics 2025 Table O (1).xlsx', sheet_name='State summary 2023-24', skiprows=0)  # Load full sheet

# Melt the data: States as rows, fuel types as columns
gwh_melted = pd.melt(gwh_df.iloc[4:],  # Start after 'Non-renewable fuels' (row 4 has fuel types)
                     id_vars=['Unnamed: 1'],  # 'Unnamed: 1' has fuel types
                     var_name='State or territory',
                     value_name='GWh')


# Rename states (based on column headers after row 1)
state_mapping = {'Unnamed: 2': 'New South Wales', 'Unnamed: 3': 'Victoria', 'Unnamed: 4': 'Queensland',
                 'Unnamed: 5': 'Western Australia', 'Unnamed: 6': 'South Australia', 'Unnamed: 7': 'Tasmania',
                 'Unnamed: 8': 'Northern Territory', 'Unnamed: 9': 'Australia'}
gwh_melted['State or territory'] = gwh_melted['State or territory'].map(state_mapping)

# Filter relevant fuel types (adjust based on exact row values; assume row 4 has headers)
fuel_types = gwh_melted[gwh_melted['Unnamed: 1'].isin(['Large-scale solar PV', 'Small-scale solar PV', 'Wind', 'Hydro', 'Bagasse, wood', 'Biogas', 'Total renewable', 'Total'])]
fuel_pivot = fuel_types.pivot(index='State or territory', columns='Unnamed: 1', values='GWh').reset_index()

# Rename columns and calculate totals/share
fuel_pivot.columns.name = None
fuel_pivot.rename(columns={'Total': 'Total Electricity'}, inplace=True)

# Convert to numeric
numeric_cols = ['Large-scale solar PV', 'Small-scale solar PV', 'Wind', 'Hydro', 'Bagasse, wood', 'Biogas', 'Total Electricity']
fuel_pivot[numeric_cols] = fuel_pivot[numeric_cols].apply(pd.to_numeric, errors='coerce')

fuel_pivot['Solar PV'] = fuel_pivot[['Large-scale solar PV', 'Small-scale solar PV']].sum(axis=1, skipna=True)
fuel_pivot['Bioenergy'] = fuel_pivot[['Bagasse, wood', 'Biogas']].sum(axis=1, skipna=True)
fuel_pivot['Total Renewables GWh'] = fuel_pivot[['Solar PV', 'Wind', 'Hydro', 'Bioenergy']].sum(axis=1, skipna=True)
fuel_pivot['Renewable Share %'] = (fuel_pivot['Total Renewables GWh'] / fuel_pivot['Total Electricity'] * 100).round(1)

# Select relevant columns
gwh_df_clean = fuel_pivot[['State or territory', 'Solar PV', 'Wind', 'Hydro', 'Bioenergy', 'Total Renewables GWh', 'Renewable Share %', 'Total Electricity']]
gwh_df_clean.to_csv('state_gwh.csv', index=False)
print("Created state_gwh.csv")

# 3. Per-Capita Renewables (Merge on full state name)
merged = pd.merge(gwh_df_clean, pop_df, on='State or territory', how='left')
merged['Per Capita Renewables GWh'] = (merged['Total Renewables GWh'] / (merged['Population'] / 1000000)).round(2)
merged.to_csv('state_renewables.csv', index=False)
print("Created state_renewables.csv")

# 4. Australian Wind Farms with Geo (from Data sheet)
wind_df = pd.read_excel('Global-Wind-Power-Tracker-February-2025.xlsx', sheet_name='Data', skiprows=0)
au_wind = wind_df[wind_df['Country/Area'] == 'Australia'][['Project Name', 'State/Province', 'Capacity (MW)', 'Status', 'Latitude', 'Longitude']]
au_wind.rename(columns={'State/Province': 'State'}, inplace=True)
au_wind.to_csv('au_wind_farms.csv', index=False)
print("Created au_wind_farms.csv")

# 5. Solar Installations by State 2024 (from swh-solar-installations-2011-to-present-and-totals.csv)
solar_csv = pd.read_csv('swh-solar-installations-2011-to-present-and-totals.csv')
cols_2024 = [col for col in solar_csv.columns if '2024' in col and 'Installation Quantity' in col]
solar_csv['Installations_2024'] = solar_csv[cols_2024].sum(axis=1, skipna=True)
postcode_df = pd.read_csv('australian_postcodes.csv')  # Ensure this file exists
solar_csv = pd.merge(solar_csv, postcode_df[['postcode', 'state']], left_on='Small Unit Installation Postcode', right_on='postcode', how='left')
state_solar = solar_csv.groupby('state')['Installations_2024'].sum().reset_index()
state_solar.to_csv('state_solar_installs.csv', index=False)
print("Created state_solar_installs.csv")

# 6. Renewable Trends (Yearly Installations from Solar CSV)
yearly_cols = [col for col in solar_csv.columns if '20' in col and 'Installation Quantity' in col]
yearly_sums = {}
for col in yearly_cols:
    year = col.split(' ')[1] if len(col.split(' ')) > 1 else 'Unknown'  # Adjust parsing
    if year not in yearly_sums:
        yearly_sums[year] = 0
    yearly_sums[year] += solar_csv[col].sum(skipna=True)

# Wind Installations
wind_install_df = pd.read_excel('sres-postcode-data-installations-2011-to-present-and-totals.xlsx', sheet_name='SGU-Wind', skiprows=3)
yearly_cols_wind = [col for col in wind_install_df.columns if ' - Installation Quantity' in col and '20' in col]
yearly_sums_wind = {}
for col in yearly_cols_wind:
    year = col.split(' ')[1]
    if year not in yearly_sums_wind:
        yearly_sums_wind[year] = 0
    yearly_sums_wind[year] += wind_install_df[col].sum(skipna=True)

# Hydro Installations
hydro_install_df = pd.read_excel('sres-postcode-data-installations-2011-to-present-and-totals.xlsx', sheet_name='SGU-Hydro', skiprows=3)
yearly_cols_hydro = [col for col in hydro_install_df.columns if ' - Installation Quantity' in col and '20' in col]
yearly_sums_hydro = {}
for col in yearly_cols_hydro:
    year = col.split(' ')[1]
    if year not in yearly_sums_hydro:
        yearly_sums_hydro[year] = 0
    yearly_sums_hydro[year] += hydro_install_df[col].sum(skipna=True)

# Combine trends
all_years = set(yearly_sums.keys()) | set(yearly_sums_wind.keys()) | set(yearly_sums_hydro.keys())
trend_data = []
for year in sorted(all_years):
    if str(year).isdigit():
        if yearly_sums.get(year, 0) > 0:
            trend_data.append({'Year': year, 'Type': 'Solar', 'Installations': yearly_sums[year]})
        if yearly_sums_wind.get(year, 0) > 0:
            trend_data.append({'Year': year, 'Type': 'Wind', 'Installations': yearly_sums_wind[year]})
        if yearly_sums_hydro.get(year, 0) > 0:
            trend_data.append({'Year': year, 'Type': 'Hydro', 'Installations': yearly_sums_hydro[year]})
trend_df = pd.DataFrame(trend_data)
trend_df.to_csv('renewable_trends.csv', index=False)
print("Created renewable_trends.csv")

# 7. Power Stations Aggregation (from Approved sheet)
approved_df = pd.read_excel('power-stations-and-projects-status.xlsx', sheet_name='Approved', skiprows=3)
approved_df = approved_df[['Power station name', 'State', 'Installed capacity (MW)', 'Fuel Source (s)']]
state_power = approved_df.groupby('State').agg({'Installed capacity (MW)': 'sum'}).reset_index()
state_power.to_csv('state_power_stations.csv', index=False)
print("Created state_power_stations.csv")

# 8. Other Renewables with Geo (from Historical sheet)
historical_df = pd.read_excel('historical-accredited-power-stations-and-projects.xlsx', sheet_name='Accredited power stations', skiprows=3)
renewable_fuels = ['Solar', 'Hydro', 'Biomass']
historical_df['Type'] = None
historical_df.loc[historical_df['Fuel source(s)'].str.contains('Solar', na=False), 'Type'] = 'Solar'
historical_df.loc[historical_df['Fuel source(s)'].str.contains('Hydro', na=False), 'Type'] = 'Hydro'
historical_df.loc[historical_df['Fuel source(s)'].str.contains('Biomass', na=False), 'Type'] = 'Bioenergy'
historical_df = historical_df[historical_df['Type'].notna()]
historical_df = historical_df[['Power station name', 'State', 'Installed capacity', 'Postcode', 'Type']]
postcode_df = pd.read_csv('australian_postcodes.csv')
historical_df['Postcode'] = historical_df['Postcode'].astype(str)
postcode_df['postcode'] = postcode_df['postcode'].astype(str)
merged = pd.merge(historical_df, postcode_df[['postcode', 'lat', 'long']], left_on='Postcode', right_on='postcode', how='left')
grouped = merged.groupby(['Postcode', 'Type', 'lat', 'long']).agg({'Installed capacity': 'sum', 'State': 'first'}).reset_index()
grouped.rename(columns={'Installed capacity': 'Capacity_MW', 'lat': 'Latitude', 'long': 'Longitude'}, inplace=True)
grouped.to_csv('other_renewables.csv', index=False)
print("Created other_renewables.csv")

print("Data processing complete. Check outputs and adjust column names if needed.")