import pandas as pd  
import time as tm 
import requests as req



class Noaa(object):

    def __init__(self, api_key):
        self._api_key = api_key
        self._header = dict(token=self._api_key)





    def _collect_all(self, url, params = None, sleep=1, df = False):
        """
                                    Collect all observations.
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
                                    DOCUMENTATION FOR PARAMS:

                    https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted
        --------------------------------------------------------------------------------
        --------------------------------------------------------------------------------
        sleep =                 Amount of wait time because each request to api. 
                                Defaults to 1.
        --------------------------------------------------------------------------------
        df =                    If True, data is returned as a Pandas DataFrame
                                If False, data is returnd at a json
        --------------------------------------------------------------------------------
                                            URLS:

        Datasets =              'https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets'
        --------------------------------------------------------------------------------
        Data Categories =       'https://www.ncdc.noaa.gov/cdo-web/api/v2/datacategories'
        --------------------------------------------------------------------------------
        Data Types =            'https://www.ncdc.noaa.gov/cdo-web/api/v2/datatypes'
        --------------------------------------------------------------------------------
        Location Categories =   'https://www.ncdc.noaa.gov/cdo-web/api/v2/locationcategories'
        --------------------------------------------------------------------------------
        Locations =             'https://www.ncdc.noaa.gov/cdo-web/api/v2/locations'
        --------------------------------------------------------------------------------
        Stations =              'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations'
        --------------------------------------------------------------------------------
        Data =                  'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid='+ dataset_id'
        --------------------------------------------------------------------------------
        """

        call = req.get(url, headers = self._header, params = params).json()
        
        total = call['metadata']['resultset']['count']
        limit = call['metadata']['resultset']['limit']
        params['offset'] = call['metadata']['resultset']['offset']
        
        cur = 0
        data = []
        self._printProgressBar(cur, total, prefix = f'{cur}/{total}', suffix = 'Complete', length = 50)
        while cur < total:
            params['offset'] = cur
            query = req.get(url, params = params, headers = self._header)
            data += query.json()['results']
            tm.sleep(sleep)
            cur += limit
            if cur > total:
                cur = total
            self._printProgressBar(cur, total, 
                                prefix = f'{cur}/{total}', 
                                suffix = 'Complete', length = 50)
        if df:
            data = pd.DataFrame(data)
            data.reset_index(inplace = True, drop = True)

        return data

    def _collect(self, url, params, sleep=1, df=False, collect_all=False):

        if collect_all:
            data =  self._collect_all(url, params, sleep=sleep, df = df)
            return data

        else:
            data = req.get(url,headers=self._header, params=params).json()
        if df:
            data= pd.DataFrame(data['results'])
        return data       
        

    def datasets(self, dataset_id = None, datatype_id = None, 
                location_id = None, station_id = None, 
                start_date = None, end_date = None, 
                sort_field = None, sort_order = None, 
                limit = None, offset = None, collect_all = False, sleep=1, df=False):
        """
                    Returns a description of available datasets
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        datasets =      If None, all datasets that fit perameters 
                        are returned. Else pass in the datasets 
                        id of interest.
        ------------------------------------------------------------------
        data_type_id =  Pass in a datatypeid  to find 
                        datasets that contain that type 
                        of data.
        ------------------------------------------------------------------
        location_id =   Pass in a locationid to find 
                        datasets that contain that 
                        data relevent to the specified 
                        location.
        ------------------------------------------------------------------
        station_id =    Pass in a stationid to find datasets that contain
                        data relevent to the specified station.
        ------------------------------------------------------------------
        start_date =    Formated date: (yyyy-mm-dd). 
                        Datasets returned will have data 
                        after the specified date. 
                        
                        Paramater can be used
                        independently of enddate.
        ------------------------------------------------------------------
        end_date =      Formated date (yyyy-mm-dd). 
                        Datasets returned will have 
                        data before the specified date. 
                        
                        Paramater can be used 
                        independently of startdate.
        ------------------------------------------------------------------
        sort_field =    The field to sort results by. 
                        Supports id, name, mindate, 
                        maxdate, and datacoverage fields.
        ------------------------------------------------------------------
        sort_order =    Which order to sort by, asc or desc. 
                        Defaults to asc.
        ------------------------------------------------------------------
        limit =         Defaults to 25. 
                        
                        Limits the number of results in the response. 
                        
                        Maximum is 1000.
        ------------------------------------------------------------------
        offset =        Defaults to 0, used to offset the resultlist.
        ------------------------------------------------------------------
        all_results =   Returns all data observations.
        ------------------------------------------------------------------
        sleep =         If all results is marked as true. 
                        Specifies the amount of time between 
                        each request call to Noaa.

                        Defaults to 1 second.
        --------------------------------------------------------------------------------
        df =            If True, data is returned as a Pandas DataFrame
                        If False, data is returnd at a json


            """
        
        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets'
        url = self._format_url(url, dataset_id)
        
        params = dict(datatypeid = datatype_id, 
                    locationid = location_id, 
                    stationid= station_id, 
                    startdate = start_date, 
                    enddate = end_date, 
                    sortfield = sort_field,
                    sortorder = sort_order,
                    limit = limit,
                    offset = offset)
            
        
        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)
            

    def data_category(self, data_category_id = None, 
                    dataset_id = None,  location_id = None, 
                    station_id = None, start_date = None, 
                    end_date = None, sort_field = None, 
                    sort_order = None, limit = None, 
                    offset = None, collect_all = False, sleep = 1, df = False):
       
        """         Returns information about data categories
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        data_category_id =  Pass in a data category id to return 
                            information on a particular data category. 
                           
                            If None, a list of all
                            available data categories will be returned.
        ------------------------------------------------------------------
        dataser_id =        Accepts a valid dataset id or a chain of 
                            dataset ids separated by ampersands. 
                            Data categories returned will be supported 
                            by dataset(s) specified
        ------------------------------------------------------------------
        location_id =       Accepts a valid location id or a chain of 
                            location ids separated by ampersands. 
                            Data categories returned will contain data 
                            for the location(s) specified
        ------------------------------------------------------------------
        station_id =        Accepts a valid station id or a chain of 
                            station ids separated by ampersands. 
                            Data categories returned will contain data 
                            for the station(s) specified
        ------------------------------------------------------------------
        start_id =          Accepts valid ISO formated date (yyyy-mm-dd). 
                            Data categories returned will have data after 
                            the specified date. 
                            
                            Paramater can be use independently of enddate
        ------------------------------------------------------------------
        end_date =          Accepts valid ISO formated date (yyyy-mm-dd). 
                            Data categories returned will have data before 
                            the specified date. 
                            
                            Paramater can be use independently of startdate
        ------------------------------------------------------------------
        sort_field =        The field to sort results by. 
                            Supports id, name, mindate, 
                            maxdate, and datacoverage fields
        ------------------------------------------------------------------
        sort_order =        Which order to sort by, asc or desc. 
                            Defaults to asc
        ------------------------------------------------------------------
        limit =             Defaults to 25, limits the number of results 
                            in the response. 

                            Maximum is 1000
        ------------------------------------------------------------------
        offset =            Defaults to 0, used to offset the resultlist. 
        ------------------------------------------------------------------
        all_results =       Returns all data observations
        ------------------------------------------------------------------
        sleep =             If all results is marked as true. 
                            sleep specifies the amount of time between 
                            each request call to Noaa.

                            Defaults to 1 second
        --------------------------------------------------------------------------------
        df =                If True, data is returned as a Pandas DataFrame
                            If False, data is returnd at a json
        """

        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/datacategories'
        url = self._format_url(url, data_category_id)

        
        params = dict(datasetid = dataset_id,
                locationid = location_id,
                stationid = station_id,
                enddate = end_date,
                stortfield = sort_field,
                sortorder = sort_order,
                limit = limit,
                offset = offset)

        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)

    def data_types(self, datatype_id = None, dataset_id = None, 
                location_id = None, station_id = None, 
                data_category_id = None, start_date = None, 
                end_date = None, sort_field = None,
                limit = None, offset = None, collect_all= False, sleep = 1, df = False):

        """
                    Returns information about datatypes. 
                    (Used to determine desired datatype ids)
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        datatype_id =       Returns information on desired datatype.
                            If None, a list of available 
                            datatypes is returned.
        ------------------------------------------------------------------
        dataset_id =        Accepts a valid dataset id or a chain 
                            of dataset ids separated by ampersands. 

                            Data types returned will be 
                            supported by dataset(s) specified.
        ------------------------------------------------------------------
        location_id =       Accepts a valid location id or a 
                            chain of location ids separated by ampersands. 
                            Data types returned will be applicable for 
                            the location(s) specified.
        ------------------------------------------------------------------
        station_id =        Accepts a valid station id or a chain 
                            of station ids separated by ampersands. 
                            Data types returned will be applicable for 
                            the station(s) specified.
        ------------------------------------------------------------------
        data_category_id =  Accepts a valid data category id 
                            or a chain of data category ids separated 
                            by ampersands (although it is rare to have a 
                            data type with more than one data category). 
                            Data types returned will be associated with 
                            the data category(ies) specified
        ------------------------------------------------------------------
        start_date =        Accepts valid ISO formated date (yyyy-mm-dd). 
                            Data types returned will have data after the 
                            specified date. 
                            
                            Paramater can be use independently of enddate.
        ------------------------------------------------------------------
        end_date =          Accepts valid ISO formated date (yyyy-mm-dd). 
                            Data types returned will have data before the 
                            specified date. 
                            
                            Paramater can be use independently of startdate.
        ------------------------------------------------------------------
        sort_field =        The field to sort results by. 
                            Supports id, name, mindate, maxdate, and 
                            datacoverage fields.
        ------------------------------------------------------------------
        sort_order =        Which order to sort by, asc or desc. 
                            Defaults to asc
        ------------------------------------------------------------------
        limit =             Defaults to 25, limits the number of 
                            results in the response. 

                            Maximum is 1000
        ------------------------------------------------------------------
        offset =            Defaults to 0, used to offset the resultlist.
        ------------------------------------------------------------------
        all_results =       Returns all data observations
        ------------------------------------------------------------------
        sleep =             If all results is marked as true. 
                            sleep specifies the amount of time between 
                            each request call to Noaa.

                            Defaults to 1 second
        --------------------------------------------------------------------------------
        df =                If True, data is returned as a Pandas DataFrame
                            If False, data is returnd at a json
        """
        
 
        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/datatypes'
        url = self._format_url(url, datatype_id)
        
        params = dict(datasetid = dataset_id,
                    loctionid = location_id,
                    stationid = station_id,
                    datacategoryid = data_category_id,
                    startdate = start_date,
                    enddate = end_date,
                    sortfield = sort_field,
                    limit = limit,
                    offset = offset)

        
        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)

    def location_categories(self, location_category = None, 
                            dataset_id = None, start_date = None,
                            end_date = None, sort_field = None,
                            sort_order = None, limit = None,
                            offset = None, collect_all= False, sleep = 1, df = False):
        
        """
                Returns information about location categories.
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        location_category = Returns information on a particular category.
                            
                            If None, a list of available location 
                            categories is returned.
        ------------------------------------------------------------------
        dataset_id =        Accepts a valid dataset id or a chain of 
                            dataset ids separated by ampersands. 
                            Location categories returned will be 
                            supported by dataset(s) specified.
        ------------------------------------------------------------------
        start_date =        Accepts valid ISO formated date (yyyy-mm-dd). 
                            Location categories returned will have data 
                            after the specified date. 

                            Paramater can be use independently of enddate.
        ------------------------------------------------------------------
        end_date =          Accepts valid ISO formated date (yyyy-mm-dd). 
                            Location categories returned will have data 
                            before the specified date. 
                            
                            Paramater can be use independently of startdate.
        ------------------------------------------------------------------
        sort_field =        The field to sort results by. 
                            Supports id, name, mindate, 
                            maxdate, and datacoverage fields.
        ------------------------------------------------------------------
        sort_order =        Which order to sort by, asc or desc. 
                            Defaults to asc.
        ------------------------------------------------------------------
        limit =             Defaults to 25, limits the number of results 
                            in the response. 

                            Maximum is 1000.
        ------------------------------------------------------------------
        offset =            Defaults to 0, used to offset the resultlist.
        ------------------------------------------------------------------
        all_results =       Returns all data observations
        ------------------------------------------------------------------
        sleep =             If all results is marked as true. 
                            sleep specifies the amount of time between 
                            each request call to Noaa
                            Defaults to 1 second
        --------------------------------------------------------------------------------
        df =                If True, data is returned as a Pandas DataFrame
                            If False, data is returnd at a json
        """

        
        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/locationcategories'
        url = self._format_url(url, location_category)

        params = dict( 
                            datasetid = dataset_id, startdate = start_date,
                            enddate = end_date, sortfield = sort_field,
                            sortorder = sort_order, limit = limit,
                            offset = offset
        )

        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)

    def locations(self, location_id = None, dataset_id = None,
                  location_category_id = None, data_category_id = None,
                  start_date = None, end_date = None,
                  sort_field = None, sort_order = None,
                  limit = None, offset = None, collect_all= False, sleep = 1, df = False):
        
        """
                    Returns information about locations.
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        location_id =          Pass in a specific location id to return 
                               information about the specified location.  
                   
                               If None, a list of all 
                               available locations is returned.
        ------------------------------------------------------------------
        dataset_id =           Accepts a valid dataset id or a chain of 
                               dataset ids separated by ampersands. 
                               Locations returned will be supported by 
                               dataset(s) specified.
        ------------------------------------------------------------------
        location_category_id = Accepts a valid location id or a chain of 
                               location category ids separated by ampersands. 
                               Locations returned will be in the location 
                               category(ies) specified.
        ------------------------------------------------------------------
        data_category_id =     Accepts a valid data category id 
                               or an array of data category IDs. 
                               Locations returned will be associated with 
                               the data category(ies) specified.
        ------------------------------------------------------------------
        start_date =           Accepts valid ISO formated date (yyyy-mm-dd). 
                               Locations returned will have data after the 
                               specified date. Paramater can be used 
                               independently of enddate.
        ------------------------------------------------------------------
        end_date =             Accepts valid ISO formated date (yyyy-mm-dd). 
                               Locations returned will have data before the 
                               specified date. Paramater can be used 
                               independently of startdate.
        ------------------------------------------------------------------
        sort_field =           The field to sort results by. 
                               Supports id, name, mindate, maxdate, 
                               and datacoverage fields.
        ------------------------------------------------------------------
        sort_order =           Which order to sort by, asc or desc. 
                               Defaults to asc.
        ------------------------------------------------------------------
        limit =                Defaults to 25, limits the number of results 
                               in the response. Maximum is 1000.
        ------------------------------------------------------------------
        offset =               Defaults to 0, used to offset the 
                               result list.
        ------------------------------------------------------------------
        all_results =          Returns all data observations
        ------------------------------------------------------------------
        sleep =                If all results is marked as true. sleep 
                               specifies the amount of time between each 
                               request call to Noaa. Defaults to 1 second
        --------------------------------------------------------------------------------
        df =                   If True, data is returned as a Pandas DataFrame
                               If False, data is returnd at a json
        """


        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/locations'
        url = self._format_url(url, location_id)



        params = dict(datasetid = dataset_id, locationcategoryid = location_category_id,
                 datacategoryid = data_category_id, startdate = start_date,
                 enddate = end_date, sortfield = sort_field,
                 sortorder = sort_order, limit = limit,
                 offset = offset)

        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)

    
    def stations(self, station_id = None, dataset_id = None, location_id = None,
                 data_category_id = None, datatype_id = None, extent = None,
                 start_date = None, end_date = None, sort_field = None,
                 sort_order = None, limit = None, offset = None, collect_all= False, 
                 sleep = 1, df = False):
        
        """
                    Returns information about weather stations.
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        station_id =       Pass in a station id to return information 
                           of the the specified weather station.

                           If None, a list of all 
                           available station is returned.
        ------------------------------------------------------------------
        dataset_id =       Accepts a valid dataset id or a chain of 
                           dataset ids separated by ampersands. 
                           Stations returned will be supported by 
                           dataset(s) specified.
        ------------------------------------------------------------------
        location_id =      Accepts a valid location id or a 
                           chain of location ids separated by ampersands. 
                           Stations returned will contain data for the 
                           location(s) specified.
        ------------------------------------------------------------------
        data_category_id = Accepts a valid data category id or an array of 
                           data category ids. Stations returned will be 
                           associated with the data category(ies) specified.
        ------------------------------------------------------------------
        datatype_id =      Accepts a valid data type id or a chain of 
                           data type ids separated by ampersands. 
                           Stations returned will contain all of the 
                           data type(s) specified.
        ------------------------------------------------------------------
        extent =           The desired geographical extent for search. 
                           Designed to take a parameter generated by 
                           Google Maps API V3 LatLngBounds.toUrlValue. 
                           Stations returned must be located within 
                           the extent specified.
        ------------------------------------------------------------------
        start_date =       Accepts valid ISO formated date (yyyy-mm-dd). 
                           Stations returned will have data after the 
                           specified date. Paramater can be use 
                           independently of enddate.
        ------------------------------------------------------------------
        end_date =         Accepts valid ISO formated date (yyyy-mm-dd). 
                           Stations returned will have data before the 
                           specified date. Paramater can be use independently 
                           of startdate.
        ------------------------------------------------------------------
        sort_field =       The field to sort results by. Supports id, name, 
                           mindate, maxdate, and datacoverage fields.
        ------------------------------------------------------------------
        sort_order =       Which order to sort by, asc or desc. Defaults to asc
        ------------------------------------------------------------------
        limit =            Defaults to 25, limits the number of 
                           results in the response. Maximum is 1000
        ------------------------------------------------------------------
        offset =           Defaults to 0, used to offset the result list.
        ------------------------------------------------------------------
        all_results =      Returns all data observations
        ------------------------------------------------------------------
        sleep =            If all results is marked as true. 
                           sleep specifies the amount of time between 
                           each request call to Noaa. Defaults to 1 second
        --------------------------------------------------------------------------------
        df =               If True, data is returned as a Pandas DataFrame
                           If False, data is returnd at a json
        """

        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations'
        url = self._format_url(url, station_id)


        params = dict(datasetid = dataset_id, locationid = location_id,
                      datacategoryid = data_category_id, datatypeid = datatype_id,
                      extent = extent, sortfield = sort_field, limit = limit,
                      offset = offset)

        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)

    def data(self, dataset_id, start_date, end_date, datatype_id = None, 
         location_id = None, station_id = None, units = None, 
         sort_field = None, sort_order = None, limit = None, 
         offset = None, include_metadata = None, collect_all= False, sleep = 1, df = False):
    
        """
                        Fetches weather data. 
        ------------------------------------------------------------------
        ------------------------------------------------------------------
        dataset_id =       REQUIRED.
                           Accepts a single valid dataset id. 
                           Data returned will be from the 
                           dataset specified.
        ------------------------------------------------------------------
        start_date =       REQUIRED
                           Accepts valid ISO formated date (YYYY-MM-DD) 
                           or date time (YYYY-MM-DDThh:mm:ss). 
                           Data returned will be after the specified date. 
                           Annual and Monthly data will be limited to 
                           a ten year range while all other data will be 
                           limted to a one year range.
        ------------------------------------------------------------------
        end_date =         REQUIRED
                           Accepts valid ISO formated date (YYYY-MM-DD) 
                           or date time (YYYY-MM-DDThh:mm:ss). 
                           Data returned will be before the specified date. 
                           Annual and Monthly data will be limited 
                           to a ten year range while all other data 
                           will be limted to a one year range.
        ------------------------------------------------------------------
        datatype_id =      Accepts a valid data type id or a chain of 
                           data type ids separated by ampersands. 
                           Data returned will contain 
                           all of the data type(s) specified.
        ------------------------------------------------------------------
        location_id =      Accepts a valid location id or a chain of location 
                           ids separated by ampersands. Data returned will 
                           contain data for the location(s) specified.
        ------------------------------------------------------------------
        station_id =       Accepts a valid station id or a chain of of station 
                           ids separated by ampersands. Data returned will 
                           contain data for the station(s) specified.
        ------------------------------------------------------------------
        units =            Accepts the literal strings 'standard' 
                           or 'metric'. Data will be scaled and 
                           converted to the specified units. 
                           If a unit is not provided then no scaling 
                           nor conversion will take place.
        ------------------------------------------------------------------
        sort_field =       The field to sort results by. 
                           Supports id, name, mindate, maxdate, 
                           and datacoverage fields.
        ------------------------------------------------------------------
        sort_order =       Which order to sort by, asc or desc. 
                           Defaults to asc.
        ------------------------------------------------------------------
        limit =            Defaults to 25, limits the number of results 
                           in the response. Maximum is 1000.
        ------------------------------------------------------------------
        offset =           Defaults to 0, used to offset the resultlist.
        ------------------------------------------------------------------
        include_metadata = Defaults to true. 
                           Used to improve response time by preventing 
                           the calculation of result metadata.
        ------------------------------------------------------------------
        all_results =      Returns all data observations
        ------------------------------------------------------------------
        sleep =            If all results is marked as true. sleep 
                           specifies the amount of time between each request 
                           call to Noaa. 
                           Defaults to 1 second
        --------------------------------------------------------------------------------
        df =               If True, data is returned as a Pandas DataFrame
                           If False, data is returnd at a json
        
        """
        
        url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid='+ dataset_id

        params = dict(datatypeid = datatype_id, locationid = location_id,
                    stationid = station_id, startdate = start_date,
                    enddate = end_date, units = units, sortfield = sort_field,
                    limit = limit, offset = offset, includemetadata = include_metadata)

        return self._collect(url, params, sleep=sleep, df=df, collect_all=collect_all)


    def _printProgressBar (self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
        """
        Citation: https://stackoverflow.com/a/34325723
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total: 
            print()

    def _format_url(self, url, id_):
        if id_:
            formatted = url + f'/{id_}'
        else:
            formatted = url + '?'

        return formatted
