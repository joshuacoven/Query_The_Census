# %%
# # Sample Script for Querying the Census
# ACS 5 Year Variables: https://api.census.gov/data/2018/acs/acs5/variables.html
# Tips:
# Different survey questions have different total population denomenators

import pandas as pd
import requests
import numpy as np

pd.options.display.max_columns = None

# insert your own key here
key = ''

################################
## county to county 5 year flows
################################
def read_census_county_to_county(key, year, var_dict):
    """read census county to county zipcode
    both sets are present
    will be a row for 01001 x 01003 AND 01003 x 01001
    """
    var = ','.join(list(var_dict))
    query = f'https://api.census.gov/data/{year}/acs/flows?get={var}&for=county:*&key={key}'
    resp = (requests.get(query).json())
    census_output_temp = (pd.DataFrame(resp[1:],columns = resp[0])).rename(columns = var_dict)
    census_output_temp['year'] = year
    return census_output_temp

year = 2019
var_dict = {'GEOID1': 'prev_fips5'
            , 'GEOID2':'fips5'
            , 'MOVEDIN': 'moved_in_to1_from2'}
census_output_temp = read_census_county_to_county(key, year, var_dict)
census = census_output_temp.query("@pd.notna(prev_fips5) & @pd.notna(fips5)").reset_index(drop = True)
census['len_geo2'] = census.prev_fips5.apply(lambda x: len(x))
census['len_geo1'] = census.fips5.apply(lambda x: len(x))
census = census.query("(len_geo2 == 5) & (len_geo1 == 5)").reset_index(drop = True)

# %%
################################
## ACS 1 year PUMS at PUMA level
################################
def query_acs1_puma(year, var_dict, key):
    """for all pumas in the us for a given year, get all variables
    have to loop through the states because doesn't let you do them all at once
    """
    var = ','.join(list(var_dict))
    census_output = pd.DataFrame()
    for st in [str(x).zfill(2) for x in range(1, 57) if x not in [3, 7, 14, 43, 52]]:
        try:
            query = f'https://api.census.gov/data/{year}/acs/acs1/pums?get={var}&for=public%20use%20microdata%20area:*&in=state:{st}&key={key}'
            #print(query)
            resp = requests.get(query).json()   
            census_output_temp = (pd.DataFrame(resp[1:],columns = resp[0])
                                .rename(columns = var_dict))
            census_output = pd.concat([census_output, census_output_temp])
            
        except:
            print(f'state {st} doesnt work')
    print(year)
    census_output['state'] = census_output['state'].str.zfill(2)
    census_output['PUMA7'] = census_output['state'] + census_output['public use microdata area'].str.zfill(5)
    print(len(census_output.PUMA7.unique()))
    census_output = census_output.drop(['PUMA7'], axis = 1)
    return census_output

puma_var_dict2021 = {'AGEP': 'age'
                 #, 'ADJINC': 'adjustment_factor_income'
                 #, 'ADJHSG': 'adj_fac_hous'
                 , 'HINCP': 'hh_income'
                 , 'SCHL':'educational_attainment'
                 , 'PWGTP': 'pums_person_weight'
                 , 'BLD': 'units_in_structure'
                 , 'GRNTP': 'gross_rent'
                 , 'RNTP': 'monthly_rent'
                 , 'TEN': 'tenure'
                 , 'VALP': 'prop_value'
                 , 'YRBLT': 'year_struct_built'
                 #, 'BDSP': 'bedrooms'
                 #, 'RMSP': 'rooms'
                 , 'ACR': 'lot_size'
                 , 'RACWHT': 'race_white'
                 , 'WGTP': 'housing_unit_weight'
                 , 'SERIALNO': 'serial_number'
                 , 'SMOCP': 'selected_monthly_owner_costs'
                 , 'NOC': 'number_children'
                 , 'BLD': 'units_in_structure'
                 # want median contract rent
                 # want home value
                 }
# anything commented out stops working 2007 and earlier
# ALL of these I get from the aggregated tables anyways
puma_var_dict = {'AGEP': 'age'
                 #, 'ADJINC': 'adjustment_factor_income'
                 #, 'ADJHSG': 'adj_fac_hous'
                 , 'HINCP': 'hh_income'
                 , 'SCHL':'educational_attainment'
                 , 'PWGTP': 'pums_person_weight'
                 , 'BLD': 'units_in_structure'
                 , 'GRNTP': 'gross_rent'
                 , 'RNTP': 'monthly_rent'
                 , 'TEN': 'tenure'
                 , 'VALP': 'prop_value'
                 , 'YBL': 'year_struct_built'
                 #, 'BDSP': 'bedrooms'
                 #, 'RMSP': 'rooms'
                 , 'ACR': 'lot_size'
                 , 'RACWHT': 'race_white'
                 , 'WGTP': 'housing_unit_weight'
                 , 'SERIALNO': 'serial_number'
                 , 'SMOCP': 'selected_monthly_owner_costs'
                 , 'NOC': 'number_children'
                 , 'BLD': 'units_in_structure'
                 # want median contract rent
                 # want home value
                 }

year = '2021'
census_output21 = query_acs1_puma(year, puma_var_dict2021, key)
# NO 2020 ACS DUE TO COVID
year = '2019'
census_output19 = query_acs1_puma(year, puma_var_dict, key)

# %%
##########################################
## ACS 5 year tables at the tract level
##########################################

def query_acs5_tract(year, var_dict, key):
    """for all tracts in the us for a given year, get all variables
    goes through each state
    56 is the highest state code
    3, 7, 14, 43, 52 all don't exist?
    sometimes I've seen it produce under 40k tracts in a year
    so include the count as a print value to check that this worked
    """

    var = ','.join(list(var_dict))
    census_output = pd.DataFrame()
    for st in [str(x).zfill(2) for x in range(1, 57) if x not in [3, 7, 14, 43, 52]]:
        try:
            query = f'https://api.census.gov/data/{year}/acs/acs5?get={var}&for=tract:*&in=state:{st}&key={key}'
            
            #print(query)
            resp = requests.get(query).json()   
            census_output_temp = (pd.DataFrame(resp[1:],columns = resp[0])
                                .rename(columns = var_dict))
            census_output = pd.concat([census_output, census_output_temp])
        except:
            print(f'state {st} doesnt work')
            print(query)
    print(f'num tracts {year}: {len(census_output)}')
    if len(census_output) < 70000:
        print('something wrong with this year: too few tracts')

    return census_output

acs_tract_dict = {'B25007_001E' : 'totHous'
            , 'B25011_002E' : 'totOwnerHous'
            , 'B25011_026E' : 'totRenterHous'
            # need house value in here too
            , 'B25077_001E': 'med_value_house'
            , 'B25064_001E': 'med_gross_rent'
            , 'B25066_002E': 'agg_gross_rent_1unit'
            , 'B25032_015E': 'tot_renter_occ_units_1attached'
            , 'B25032_014E': 'tot_renter_occ_units_1deattached'
            # I think I want to exclude these. the years change for each cut (of course)
            # they also don't change in a completely standard way like (last 5 years, last 10 years, etc.)
            # better to do median age of the housing stock, owned or rented
            # can use year fixed effects to adjust for the fact that time moves on
            #, 'B25036_013E': 'tot_renter_occ_year_built'
            #, 'B25036_014E': 'tot_renter_occ_year_built_2014_later'
            #, 'B25036_015E': 'tot_renter_occ_year_built_2010_2013'
            #, 'B25036_016E': 'tot_renter_occ_year_built_2000_2009'
            #, 'B25036_017E': 'tot_renter_occ_year_built_1990_1999'
            , 'B25042_009E': 'tot_renter_occ_bedrooms'
            , 'B25042_010E': 'tot_renter_occ_no_bedrooms'
            , 'B25042_011E': 'tot_renter_occ_one_bedroom'
            , 'B25042_012E': 'tot_renter_occ_two_bedrooms'
            , 'B25042_013E': 'tot_renter_occ_three_bedrooms'
            , 'B25042_014E': 'tot_renter_occ_four_bedrooms'
            , 'B25042_015E': 'tot_renter_occ_five_bedrooms'
            , 'B25118_014E': 'tot_renter_inc'
            , 'B25118_015E': 'tot_renter_inc_less5000'
            , 'B25118_016E': 'tot_renter_inc_5000_9999'
            , 'B25118_017E': 'tot_renter_inc_10000_14999'
            , 'B25118_018E': 'tot_renter_inc_15000_19999'
            , 'B25118_019E': 'tot_renter_inc_20000_34999'
            , 'B25118_020E': 'tot_renter_inc_35000_49999'
            , 'B25118_021E': 'tot_renter_inc_50000_74999'
            , 'B25118_022E': 'tot_renter_inc_75000_99999'
            , 'B25118_023E': 'tot_renter_inc_100000_149999'
            , 'B25118_024E': 'tot_renter_inc_150000plus'
            , 'B14003_016E' : 'm_1517_enr_priv'
            , 'B14003_007E' : 'm_1517_enr_pub'
            , 'B14003_025E' : 'm_1517_not_enr'
            , 'B14003_044E' : 'f_1517_enr_priv'
            , 'B14003_035E' : 'f_1517_enr_pub'
            , 'B14003_053E' : 'f_1517_not_enr'
            , 'B08012_001E' : 'time_total'
            , 'B08012_013E' : 'time_90plus'
            , 'B08012_012E' : 'time_60_89'
            , 'B08012_011E' : 'time_45_59'
            , 'B08122_001E' : 'means_total'
            , 'B08122_013E': 'means_publictransp'
            , 'B08122_017E' : 'means_walked'
            , 'B25037_002E': 'med_year_built_ooc'
            , 'B25037_003E': 'med_year_built_rent'}


year = '2018'
census_output = query_acs5_tract(year, acs_tract_dict, key)

# %%
##########################
## PUMA land area
##########################
query = 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2018/MapServer/0/query?where=AREALAND+%3E+-1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=STATE%2C+PUMA%2C+AREALAND&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=json'
# http request
try:
    resp = (requests.get(query).json())

    ## turn into dataframe
    resp = resp#['features'][0]
    #census_output_temp = pd.DataFrame.from_dict(resp).T.reset_index(drop = True)
    census_output = pd.DataFrame([resp['features'][i]['attributes'] for i in range(len(resp['features']))])
except IndexError:
    pass
    
#area is in 1/1000th's of km's, need to convert below. confirmed densities with census reporter
#convert to sq miles
census_output['AREALAND'] = census_output['AREALAND']/(1000000*1.609*1.609)
census_output['PUMA7'] = census_output['STATE'] + census_output['PUMA']

# %%
#########################
## ACS 5 block group level
#########################

def query_acs5_block(year, var_dict, key):
    """for all tracts in the us for a given year, get all variables
    goes through each state
    56 is the highest state code
    3, 7, 14, 43, 52 all don't exist?
    sometimes I've seen it produce under 40k tracts in a year
    so include the count as a print value to check that this worked
    """
    var = ','.join(list(var_dict))
    census_output = pd.DataFrame()
    for st in [str(x).zfill(2) for x in range(1, 57) if x not in [3, 7, 14, 43, 52]]:
        try:
            query = f'https://api.census.gov/data/{year}/acs/acs5?get={var}&for=block%20group:*&in=state:{st}&in=county:*&in=tract:*&key={key}'
                
            #print(query)
            resp = requests.get(query).json()   
            census_output_temp = (pd.DataFrame(resp[1:],columns = resp[0])
                                .rename(columns = var_dict))
            census_output = pd.concat([census_output, census_output_temp])
        except:
            print(f'state {st} doesnt work')
    print(f'num blockgroups {year}: {len(census_output)}')
    if len(census_output) < 200000:
        print('something wrong with this year: too few blockgroups')
    return census_output

acs_block_dict = {'B02001_001E': 'demo_total_pop'
                , 'B02001_002E': 'demo_white'
                , 'B02009_001E': 'demo_black'
               , 'B03002_012E': 'demo_hispanic'
               , 'B19013_001E': 'household_income'
               ,'B25007_001E' : 'totHous'
            , 'B25011_002E' : 'totOwnerHous'
            , 'B25011_026E' : 'totRenterHous'
            , 'B25064_001E': 'med_gross_rent'
            , 'B25066_002E': 'agg_gross_rent_1unit'
            , 'B25032_015E': 'tot_renter_occ_units_1attached'
            , 'B25032_014E': 'tot_renter_occ_units_1deattached'
            , 'B25036_013E': 'tot_renter_occ_year_built'
            , 'B25042_009E': 'tot_renter_occ_bedrooms'
            , 'B25042_010E': 'tot_renter_occ_no_bedrooms'
            , 'B25042_011E': 'tot_renter_occ_one_bedroom'
            , 'B25042_012E': 'tot_renter_occ_two_bedrooms'
            , 'B25042_013E': 'tot_renter_occ_three_bedrooms'
            , 'B25042_014E': 'tot_renter_occ_four_bedrooms'
            , 'B25042_015E': 'tot_renter_occ_five_bedrooms'
            , 'B25007_012E': 'tot_renter_occ_age'
            , 'B25007_013E': 'tot_renter_occ_age_15_24'
            , 'B25007_014E': 'tot_renter_occ_age_25_34'
            , 'B25007_015E': 'tot_renter_occ_age_35_44'
            , 'B25007_016E': 'tot_renter_occ_age_45_54'
            , 'B25007_017E': 'tot_renter_occ_age_55_59'
            , 'B25007_018E': 'tot_renter_occ_age_60_64'
            , 'B25007_019E': 'tot_renter_occ_age_65_74'
            , 'B25007_020E': 'tot_renter_occ_age_75_84'
            , 'B25007_021E': 'tot_renter_occ_age_85plus'
            , 'B25118_014E': 'tot_renter_inc'
            , 'B25037_002E': 'med_year_built_ooc'
            , 'B25037_003E': 'med_year_built_rent'
            , 'B14003_016E' : 'm_1517_enr_priv'
            , 'B14003_007E' : 'm_1517_enr_pub'
            , 'B14003_025E' : 'm_1517_not_enr'
            , 'B14003_044E' : 'f_1517_enr_priv'
            , 'B14003_035E' : 'f_1517_enr_pub'
            , 'B14003_053E' : 'f_1517_not_enr'
            , 'B08012_001E' : 'time_total'
            , 'B08012_013E' : 'time_90plus'
            , 'B08012_012E' : 'time_60_89'
            , 'B08012_011E' : 'time_45_59'
            , 'B08122_001E' : 'means_total'
            , 'B08122_013E': 'means_publictransp'
            , 'B08122_017E' : 'means_walked'
            }

year = '2018'
census_output = query_acs5_block(year, acs_block_dict, key)

# %%
#########################
## ACS 5 PUMA level 
#########################
def query_acs5_puma(year, var_dict, key):
    """for all pumas in the us for a given year, get all variables
    """

    var = ','.join(list(var_dict))
    census_output = pd.DataFrame()
    for st in [str(x).zfill(2) for x in range(1, 57) if x not in [3, 7, 14, 43, 52]]:
        try:
            query = f'https://api.census.gov/data/{year}/acs/acs5?get={var}&for=public%20use%20microdata%20area:*&in=state:{st}&key={key}'
            
            #print(query)
            resp = requests.get(query).json()   
            census_output_temp = (pd.DataFrame(resp[1:],columns = resp[0])
                                .rename(columns = var_dict))
            census_output = pd.concat([census_output, census_output_temp])
        except:
            print(f'state {st} doesnt work')
    return census_output

acs_puma_dict = {'B25007_001E' : 'totHous'
            , 'B25011_002E' : 'totOwnerHous'
            , 'B25011_026E' : 'totRenterHous'
            , 'B25064_001E': 'med_gross_rent'
            , 'B25066_002E': 'agg_gross_rent_1unit'
            , 'B25032_015E': 'tot_renter_occ_units_1attached'
            , 'B25032_014E': 'tot_renter_occ_units_1deattached'
            , 'B25037_002E': 'med_year_built_ooc'
            , 'B25037_003E': 'med_year_built_rent'
            , 'B25042_009E': 'tot_renter_occ_bedrooms'
            , 'B25042_010E': 'tot_renter_occ_no_bedrooms'
            , 'B25042_011E': 'tot_renter_occ_one_bedroom'
            , 'B25042_012E': 'tot_renter_occ_two_bedrooms'
            , 'B25042_013E': 'tot_renter_occ_three_bedrooms'
            , 'B25042_014E': 'tot_renter_occ_four_bedrooms'
            , 'B25042_015E': 'tot_renter_occ_five_bedrooms'
            , 'B25118_014E': 'tot_renter_inc'
            , 'B25118_015E': 'tot_renter_inc_less5000'
            , 'B25118_016E': 'tot_renter_inc_5000_9999'
            , 'B25118_017E': 'tot_renter_inc_10000_14999'
            , 'B25118_018E': 'tot_renter_inc_15000_19999'
            , 'B25118_019E': 'tot_renter_inc_20000_34999'
            , 'B25118_020E': 'tot_renter_inc_35000_49999'
            , 'B25118_021E': 'tot_renter_inc_50000_74999'
            , 'B25118_022E': 'tot_renter_inc_75000_99999'
            , 'B25118_023E': 'tot_renter_inc_100000_149999'
            , 'B25118_024E': 'tot_renter_inc_150000plus'
            , 'B14003_016E' : 'm_1517_enr_priv'
            , 'B14003_007E' : 'm_1517_enr_pub'
            , 'B14003_025E' : 'm_1517_not_enr'
            , 'B14003_044E' : 'f_1517_enr_priv'
            , 'B14003_035E' : 'f_1517_enr_pub'
            , 'B14003_053E' : 'f_1517_not_enr'
            , 'B08012_001E' : 'time_total'
            , 'B08012_013E' : 'time_90plus'
            , 'B08012_012E' : 'time_60_89'
            , 'B08012_011E' : 'time_45_59'
            , 'B08122_001E' : 'means_total'
            , 'B08122_013E': 'means_publictransp'
            , 'B08122_017E' : 'means_walked'}

year = '2019'
census_output = query_acs5_puma(year, acs_puma_dict, key)
