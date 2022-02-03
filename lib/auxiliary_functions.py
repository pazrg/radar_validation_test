import requests
import json
from sklearn.metrics import mean_squared_error
import geopy
from geopy.distance import geodesic
import pandas as pd
import os
import numpy as np


api_key = 'api_dummy' #write you SOCIB api_key
headers = {
    'accept': 'application/vnd.socib+json',
    'apikey': api_key,
}

def bbox(center, radious):
    lat1, lon1 = center.split(',')
    origin = geopy.Point(lat1, lon1)
    #bearing (float) – Bearing in degrees: 0 – North, 90 – East, 180 – South, 270 or -90 – West.
    min_lon = (geodesic(kilometers=int(radious)).destination(origin, -90)).longitude
    max_lon = (geodesic(kilometers=int(radious)).destination(origin, 90)).longitude
    min_lat = (geodesic(kilometers=int(radious)).destination(origin, 180)).latitude
    max_lat = (geodesic(kilometers=int(radious)).destination(origin, 0)).latitude
    #min. lat., max. lat., min. lon., max. lon
    return ','.join([str(min_lat), str(max_lat), str(min_lon), str(max_lon)])

def sources_lookup(period, variable, center, radious):
    box = bbox(center, radious)
    url = 'https://api.socib.es/data-sources/?bbox=%s&initial_datetime=%s&end_datetime=%s&standard_name=%s&limit=100&subtype=deployment'%(box,period[0], period[1],variable)
    request = requests.get(url, headers=headers)
    response = json.loads(request.text)
    print('matches:'+str(response['count']))
    return response['results']

def series(s,response, standard_name, qc_max, center, parse_units, parse_series):
    dfs = []
    for f in response:
        for v in f['variables']:
            condition = v['standard_name'] == standard_name
            if condition:
                data = parse_series(s,f,v,qc_max,center)
                if data is not None:
                    df = pd.DataFrame(data)
                    df.fillna(value=np.nan, inplace = True)
                    n = [i for i in list(df.columns) if i != 'time'][0]
                    df = parse_units(df,n,v['units'])
                    dfs.append(df)
    return dfs

def distance(lat2,lon2,center):
    lat1,lon1 = center.split(',')
    origin = geopy.Point(lat1, lon1)
    destiny = geopy.Point(lat2, lon2)
    return geopy.distance.distance(origin, destiny).m

def sources_timeseries(sources, standard_name, resolution, center, radious, qc_max, period, parse_units, parse_series):
    full_dfs = []
    for s in sources:
        feature_types = ' '.join(s['feature_types'])
        if 'grid' in feature_types:
            box = bbox(center, 2)
            url = 'https://api.socib.es/data-sources/%s/data/?initial_datetime=%s&end_datetime=%s&processing_level=L1*&bbox=%s&resample_how=mean&resample_rule=%s&standard_name=%s'%(s['id'], period[0], period[1], box, resolution, standard_name)
        else:
            url = 'https://api.socib.es/data-sources/%s/data/?initial_datetime=%s&end_datetime=%s&processing_level=L1*&elevation_range=-5,0&resample_how=mean&resample_rule=%s&standard_name=%s'%(s['id'],  period[0], period[1], resolution, standard_name)
        if qc_max is not None:
            url = url +'&max_qc_value='+qc_max
        try:
            request = requests.get(url, headers=headers)
            response = json.loads(request.text)
            dfs = series(s,response, standard_name, qc_max, center,parse_units, parse_series)
            full_dfs.append(dfs)
        except Exception as e:
            pass
    full_dfs = sum(full_dfs,[])
    dfs = [df.set_index('time') for df in full_dfs if df is not None]
    return pd.concat(dfs, axis=1)


def qcpercentage(sources, standard_name, resolution, center, radious, qc_max, period, parse_units, parse_series):
    buffer = {}
    timeseries_qcmax2 = sources_timeseries(sources, standard_name, resolution, center, radious, qc_max, period, parse_units, parse_series)
    timeseries_qcmax9 = sources_timeseries(sources, standard_name, resolution, center, radious, None, period, parse_units, parse_series)
    cols1 = timeseries_qcmax2.columns
    cols2 = timeseries_qcmax9.columns
    for i in range(0, len(cols1)):
        qc9_nonans = timeseries_qcmax9.describe()[cols2[i]]['count']
        qc2_nonans = timeseries_qcmax2.describe()[cols1[i]]['count']
        val = (qc2_nonans*100)/(qc9_nonans)
        name = '|'.join(cols1[i].split('|')[:2])
        buffer[name] = val 
    df = pd.DataFrame(buffer, index=['%QC=1,2'])
    return df

def mse(timeseries):
    dfs = []
    cols = list(timeseries.columns)
    for i in cols:
        values = []
        for j in cols:
            subset = timeseries.loc[:, [i,j]]
            subset.dropna(inplace = True)
            values.append(mean_squared_error(subset[i], subset[j]))
        df = pd.DataFrame([values], columns=cols, index = [i])
        dfs.append(df)
    return pd.concat(dfs)