#!/usr/bin/python3

import argparse
import csv
from typing import List, Optional, Any
import re
import datetime
import calendar

FLOAT_NAN = float("nan")

dateParserRegex = re.compile(r"(?P<year>\d\d\d\d)\-(?P<month>\d\d)\-(?P<day>\d\d)")

def is_leap_year(year):
    """ if year is a leap year return True
        else return False """
    if year % 100 == 0:
        return year % 400 == 0
    return year % 4 == 0

def doy(Y,M,D):
    """ given year, month, day return day of year
        Astronomical Algorithms, Jean Meeus, 2d ed, 1998, chap 7 """
    if is_leap_year(Y):
        K = 1
    else:
        K = 2
    N = int((275 * M) / 9.0) - K * int((M + 9) / 12.0) + D - 30
    return N

class CSVHeaders:
    header_to_index_map : dict [str,int] = {}
    headers : List[str]
    
    def __init__(self, header_row : List[str]) -> None:
        self.headers = header_row.copy()
        
        i = 0
        for s in header_row:
            self.header_to_index_map[s] = i
            i += 1
    
    def GetIndexFromColumnName(self, value : str ) -> int:
        return self.header_to_index_map[value]

    def GetColumnNameFromIndex(self, value: int) -> str:
        return self.headers[value]

    def Count(self) -> int:
        return len(self.headers)

    def Items(self) -> List[str]:
        return self.headers 

    def __getitem__(self, key : str) -> int:
        return self.header_to_index_map[key]

class NOAAClimateDataHeaders:
    DateIndex : int
    StationIDIndex: int
    StationNameIndex: int 
    ACMHIndex: int
    PRCPIndex: int
    PSUNIndex: int
    TAVGIndex: int
    TMAXIndex: int
    TMINIndex: int
    TOBSIndex: int

    def __init__(self, headers : CSVHeaders) -> None:
        self.DateIndex = int(headers["DATE"])
        self.StationIDIndex = int(headers["STATION"])
        self.StationNameIndex = int(headers["NAME"])
        self.ACMHIndex = int(headers["ACMH"])
        self.PRCPIndex = int(headers["PRCP"])
        self.PSUNIndex = int(headers["PSUN"])
        self.TAVGIndex = int(headers["TAVG"])
        self.TMAXIndex = int(headers["TMAX"])
        self.TMINIndex = int(headers["TMIN"])
        self.TOBSIndex = int(headers["TOBS"])

class DataEntry:
    date : datetime.date
    year: int = 0
    month: int = 0
    day: int = 0
    tmax : float = FLOAT_NAN
    tmin: float = FLOAT_NAN
    tavg: float = FLOAT_NAN

    def __init__(self, row : List[str], noaa_headers : NOAAClimateDataHeaders ) -> None:
        textdate = row[noaa_headers.DateIndex]
        match = dateParserRegex.match(textdate)
        groups = match.groupdict()
        self.year = int(groups["year"])
        self.month = int(groups["month"])
        self.day = int(groups["day"])
        self.date = datetime.date(self.year, self.month, self.day)

        max_temp = row[noaa_headers.TMAXIndex]
        min_temp = row[noaa_headers.TMINIndex]
        avg_temp = row[noaa_headers.TAVGIndex]
        self.tmax = max_temp if max_temp else FLOAT_NAN
        self.tmin = min_temp if min_temp else FLOAT_NAN
        self.tavg = avg_temp if avg_temp else FLOAT_NAN    

class StationData:
    station_id: str
    station_name: str
    values : List[DataEntry]

    def __init__(self, id : str) -> None:
        self.station_id = id
        self.station_name = ''
        self.values = list[DataEntry]()

    def GetYears(self) -> set[int]:
        years = set[int]()
        for value in self.values:
            years.add(value.year)
        
        return sorted(years)

class Corpus:
    stations: List[StationData] = list()
    station_id_to_data: dict[str,StationData] = dict()
    
    def __getitem__(self, station_id: str) -> Optional[StationData]:
        station = self.station_id_to_data.get(station_id)
        if station is not None:
            return station
        
        station = StationData(station_id)
        self.station_id_to_data[station_id] = station
        self.stations.append(station)
        return station


def parse(inputfile : str) -> None:
    corpus = Corpus()

    # Read the CSV file headers, then the file itself, and store
    # data collated by Station ID

    with open(inputfile, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        headers = CSVHeaders(reader.__next__())
        noaa_headers = NOAAClimateDataHeaders(headers)

        last_station : Optional[StationData] = None

        for row in reader:
            data_entry = DataEntry(row, noaa_headers)
            row_station_id = row[noaa_headers.StationIDIndex]
            if last_station is None or last_station.station_id != row_station_id:
                last_station = corpus[row_station_id]
                if not last_station.station_name:
                    last_station.station_name = row[noaa_headers.StationNameIndex]
            
            last_station.values.append(data_entry)
    
    # All of the data rows have been read in now. Emit each station's data
    # as a matrix of day of the year (rows) vs columns (years)

    for station in corpus.stations:
        # Get a sorted list of years from the data-set.
        year_list = station.GetYears()
        year_dict = dict[int,int]()
        year_count = 0
        for year in year_list:
            year_dict[year] = year_count
            year_count += 1   

        # Make an array of days of the year (0-364) vs. years.

        data_entries = [['' for y in range(year_count)] for x in range(365)]   

        for data in station.values:
            # Skip leap year data
            if data.month == 2 and data.day == 29:
                continue

            day_of_year = doy(data.year, data.month, data.day)
            
            if is_leap_year(data.year) and data.month > 2:
                day_of_year -= 1

            year_index = year_dict[data.year]
            data_entries[day_of_year-1][year_index] = data.tmax if (not data.tmax == FLOAT_NAN) else '="MISSING"'

        with open(f'{station.station_id}.csv', 'w', newline='', encoding='utf-8') as csvoutputfile:
            writer = csv.writer(csvoutputfile, dialect=csv.excel)
            writer.writerows([
                [f"Maximum daily temperatures from {station.station_name} (Station ID: {station.station_id}), from {year_list[0]} to {year_list[len(year_list)-1]}"],
                [f"Data From NOAA (https://www.ncdc.noaa.gov/)"],[],
                [f"Generated on {datetime.date.today()}"],[]
            ])

            header_row = ["DOY","Date"] + year_list
            writer.writerow(header_row)
            for i in range(len(data_entries)):
                date = datetime.date(1975,1,1)
                days_to_add = i
                monthday = date + datetime.timedelta(days_to_add)

                monthname = calendar.month_abbr[monthday.month]
                newrow = [i+1,f'{monthname}-{monthday.day}'] + data_entries[i]

                writer.writerow(newrow)


def main() -> None:
    parser = argparse.ArgumentParser()

    # Required positional argument
    parser.add_argument("csvfile", help="CSV File to process")
    args = parser.parse_args()
    
    parse(args.csvfile)
    

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()
