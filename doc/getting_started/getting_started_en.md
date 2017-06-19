## Introduction

For a start, we need a computer. For example, let's create virtual instance in Openstack with following characteristics:
```
RAM             61GB
VCPUs           16 VCPU
Disk            40GB
Ephemeral Disk  100GB
```

OS:
```
$ lsb_release -a
No LSB modules are available.
Distributor ID:	Ubuntu
Description:	Ubuntu 16.04 LTS
Release:	16.04
Codename:	xenial
```

We gonna have a try with open data from On Time database, which is arranged by United States Department of
Transportation). One can find information about it, structure of the table and examples of queries here:
```
https://github.com/yandex/ClickHouse/blob/master/doc/example_datasets/1_ontime.txt
```

## Building

To build ClickHouse we will use a manual located here:
```
https://github.com/yandex/ClickHouse/blob/master/doc/build.md
```

Install required packages. After that let's run the following command from directory with source code of ClickHouse:
```
~/ClickHouse$ ./release
```

The build successfully completed:
![](images/build_completed.png)

Installing packages and running ClickHouse:
```
sudo apt-get install ../clickhouse-server-base_1.1.53960_amd64.deb ../clickhouse-server-common_1.1.53960_amd64.deb
sudo apt-get install ../clickhouse-client_1.1.53960_amd64.deb
sudo service clickhouse-server start
```

## Creation of table

Before loading the date into ClickHouse, let's start console client of ClickHouse in order to create a table with necessary fields:
```
$ clickhouse-client
```

The table is being created with following query:
```
:) create table `ontime` (
  `Year` UInt16,
  `Quarter` UInt8,
  `Month` UInt8,
  `DayofMonth` UInt8,
  `DayOfWeek` UInt8,
  `FlightDate` Date,
  `UniqueCarrier` FixedString(7),
  `AirlineID` Int32,
  `Carrier` FixedString(2),
  `TailNum` String,
  `FlightNum` String,
  `OriginAirportID` Int32,
  `OriginAirportSeqID` Int32,
  `OriginCityMarketID` Int32,
  `Origin` FixedString(5),
  `OriginCityName` String,
  `OriginState` FixedString(2),
  `OriginStateFips` String,
  `OriginStateName` String,
  `OriginWac` Int32,
  `DestAirportID` Int32,
  `DestAirportSeqID` Int32,
  `DestCityMarketID` Int32,
  `Dest` FixedString(5),
  `DestCityName` String,
  `DestState` FixedString(2),
  `DestStateFips` String,
  `DestStateName` String,
  `DestWac` Int32,
  `CRSDepTime` Int32,
  `DepTime` Int32,
  `DepDelay` Int32,
  `DepDelayMinutes` Int32,
  `DepDel15` Int32,
  `DepartureDelayGroups` String,
  `DepTimeBlk` String,
  `TaxiOut` Int32,
  `WheelsOff` Int32,
  `WheelsOn` Int32,
  `TaxiIn` Int32,
  `CRSArrTime` Int32,
  `ArrTime` Int32,
  `ArrDelay` Int32,
  `ArrDelayMinutes` Int32,
  `ArrDel15` Int32,
  `ArrivalDelayGroups` Int32,
  `ArrTimeBlk` String,
  `Cancelled` UInt8,
  `CancellationCode` FixedString(1),
  `Diverted` UInt8,
  `CRSElapsedTime` Int32,
  `ActualElapsedTime` Int32,
  `AirTime` Int32,
  `Flights` Int32,
  `Distance` Int32,
  `DistanceGroup` UInt8,
  `CarrierDelay` Int32,
  `WeatherDelay` Int32,
  `NASDelay` Int32,
  `SecurityDelay` Int32,
  `LateAircraftDelay` Int32,
  `FirstDepTime` String,
  `TotalAddGTime` String,
  `LongestAddGTime` String,
  `DivAirportLandings` String,
  `DivReachedDest` String,
  `DivActualElapsedTime` String,
  `DivArrDelay` String,
  `DivDistance` String,
  `Div1Airport` String,
  `Div1AirportID` Int32,
  `Div1AirportSeqID` Int32,
  `Div1WheelsOn` String,
  `Div1TotalGTime` String,
  `Div1LongestGTime` String,
  `Div1WheelsOff` String,
  `Div1TailNum` String,
  `Div2Airport` String,
  `Div2AirportID` Int32,
  `Div2AirportSeqID` Int32,
  `Div2WheelsOn` String,
  `Div2TotalGTime` String,
  `Div2LongestGTime` String,
  `Div2WheelsOff` String,
  `Div2TailNum` String,
  `Div3Airport` String,
  `Div3AirportID` Int32,
  `Div3AirportSeqID` Int32,
  `Div3WheelsOn` String,
  `Div3TotalGTime` String,
  `Div3LongestGTime` String,
  `Div3WheelsOff` String,
  `Div3TailNum` String,
  `Div4Airport` String,
  `Div4AirportID` Int32,
  `Div4AirportSeqID` Int32,
  `Div4WheelsOn` String,
  `Div4TotalGTime` String,
  `Div4LongestGTime` String,
  `Div4WheelsOff` String,
  `Div4TailNum` String,
  `Div5Airport` String,
  `Div5AirportID` Int32,
  `Div5AirportSeqID` Int32,
  `Div5WheelsOn` String,
  `Div5TotalGTime` String,
  `Div5LongestGTime` String,
  `Div5WheelsOff` String,
  `Div5TailNum` String
) ENGINE = MergeTree(FlightDate, (Year, FlightDate), 8192)
``` 
![](images/table_created.png)

One can get information about table using following queries:
```
:) desc ontime
```

```
:) show create ontime
```

## Filling in the data

Let's download the data:
```
for s in `seq 1987 2015`; do
	for m in `seq 1 12`; do
		wget http://tsdata.bts.gov/PREZIP/On_Time_On_Time_Performance_${s}_${m}.zip
	done
done
```

After that, add the data to ClickHouse:
```
for i in *.zip; do
	echo $i
	unzip -cq $i '*.csv' | sed 's/\.00//g' | clickhouse-client --query="insert into ontime format CSVWithNames"
done
```

## Working with data

Let's check if the table contains something:
```
:) select FlightDate, FlightNum, OriginCityName, DestCityName from ontime limit 10; 
```

![](images/something.png)

Now we'll craft more complicated query. E.g., output fraction of flights delayed for more than 10 minutes, for each year:
```
select Year, c1/c2
from
(
	select
		Year,
		count(*)*100 as c1
	from ontime
	where DepDelay > 10
	group by Year
)
any inner join
(
	select
		Year,
		count(*) as c2
	from ontime
	group by Year
) using (Year)
order by Year;
```

![](images/complicated.png)

## Additionally

### Copying a table

Suppose we need to copy 1% of records (luckiest ones) from ```ontime``` table to new table named ```ontime_ltd```.
To achieve that make the queries:
```
:) create table ontime_ltd as ontime;
:) insert into ontime_ltd select * from ontime where rand() % 100 = 42;
```

### Working with multiple tables

If one need to collect data from multiple tables, use function ```merge(database, regexp)```:
```
:) select (select count(*) from merge(default, 'ontime.*'))/(select count() from ontime);
```

![](images/1_percent.png)

### List of running queries

For diagnostics one usually need to know what exactly ClickHouse is doing at the moment. Let's execute a query that gonna last long:
```
:) select sleep(1000);
```

If we run ```clickhouse-client``` after that in another terminal, one can output list of queries and some additional useful information about them:
```
:) show processlist;
```

![](images/long_query.png)
