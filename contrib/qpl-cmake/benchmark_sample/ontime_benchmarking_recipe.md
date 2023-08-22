# CLICKHOUSE-IAA
 [CLICKHOUSE](https://clickhouse.com/) is an open-source column-oriented database management system (DBMS) for online analytical processing of queries (OLAP). ClickHouse gained the new "DeflateQpl" compression codec which utilizes the Intel® IAA offloading technology to provide a high-performance DEFLATE implementation. The codec uses the Intel® Query Processing Library (QPL) which abstracts access to the hardware accelerator, respectively to a software fallback in case the hardware accelerator is not available. DEFLATE provides in general higher compression rates than ClickHouse's LZ4 default codec, and as a result, offers less disk I/O and lower main memory consumption. Refer to [36654](https://github.com/ClickHouse/ClickHouse/pull/36654) [DEFLATE_QPL](https://clickhouse.com/docs/en/sql-reference/statements/create/table#deflate_qpl)

# Build
- Clone repository with specified branch: 
``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git -b intel_accelerators
```
- Recommend use Clang-14 to build because the baseline in this branch is Clickhouse 21.12. For  build instructions and other generic requirements, please refer to Clickhouse official [build instructions](https://clickhouse.com/docs/en/development/build)
- Make sure your working machine meet the QPL required [prerequisites](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#prerequisites)
- deflate_qpl is enabled by default during cmake build. In case you accidentally change it, please double-check build flag: ENABLE_QPL=1

# Benchmark

## Files list

The folders `benchmark_sample` under [qpl-cmake](https://github.com/ClickHouse/ClickHouse/tree/intel_accelerators/contrib/qpl-cmake) give example to run benchmark with python scripts:

`client_scripts` contains scripts and files to run benchmark:
- `client_stressing_test.py`: The python script for query stress test with [1~4] server instances.
- `queries_ontime.sql`: The file lists all queries for [Ontime](https://clickhouse.com/docs/en/getting-started/example-datasets/ontime)
- `allin1_ontime.sh`: This shell script executes benchmark workflow all in one automatically.

`database_files` means it will store database files according to lz4/deflate/zstd codec.

## Definition

[CLICKHOUSE_EXE] means the executable binary of clickhouse.
[INSTANCE] means a individual server instance connected with respective client by specifying port.

## Environment

- CPU: The 4th Generation Intel Xeon Scalable processors, codenamed Sapphire Rapids.
- OS Requirements refer to [System Requirements for QPL](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#system-requirements)
- IAA Setup refer to [Accelerator Configuration](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#accelerator-configuration)
- Install python modules:

``` bash
pip3 install clickhouse_driver numpy
```

[Self-check for IAA]

``` bash
$ accel-config list | grep -P 'iax|state'
```

Expected output like this:
``` bash
    "dev":"iax1",
    "state":"enabled",
            "state":"enabled",
```

If you see nothing output, it means IAA is not ready to work. Please check IAA setup again.

## Generate raw data

``` bash
$ cd ./benchmark_sample && mkdir rawdata_dir && cd rawdata_dir
$ curl -O https://clickhouse-datasets.s3.yandex.net/ontime/partitions/ontime.tar
$ tar -xvf ontime.tar
$ [CLICKHOUSE_EXE] server >&/dev/null&
$ [CLICKHOUSE_EXE] client --query="select * from datasets.ontime FORMAT CSV" > ontime.csv
```

## Database setup

Set up database with LZ4 codec for the first [INSTANCE]

``` bash
$ cd ./benchmark_sample/database_dir/lz4
$ [CLICKHOUSE_EXE] server -C config_lz4.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9000
```

Here you should see the message `Connected to ClickHouse server` from console which means client successfully setup connection with server.

Creating a table with the following query from console:
    CREATE TABLE `ontime`
    (
        `Year`                            UInt16,
        `Quarter`                         UInt8,
        `Month`                           UInt8,
        `DayofMonth`                      UInt8,
        `DayOfWeek`                       UInt8,
        `FlightDate`                      Date,
        `Reporting_Airline`               LowCardinality(String),
        `DOT_ID_Reporting_Airline`        Int32,
        `IATA_CODE_Reporting_Airline`     LowCardinality(String),
        `Tail_Number`                     LowCardinality(String),
        `Flight_Number_Reporting_Airline` LowCardinality(String),
        `OriginAirportID`                 Int32,
        `OriginAirportSeqID`              Int32,
        `OriginCityMarketID`              Int32,
        `Origin`                          FixedString(5),
        `OriginCityName`                  LowCardinality(String),
        `OriginState`                     FixedString(2),
        `OriginStateFips`                 FixedString(2),
        `OriginStateName`                 LowCardinality(String),
        `OriginWac`                       Int32,
        `DestAirportID`                   Int32,
        `DestAirportSeqID`                Int32,
        `DestCityMarketID`                Int32,
        `Dest`                            FixedString(5),
        `DestCityName`                    LowCardinality(String),
        `DestState`                       FixedString(2),
        `DestStateFips`                   FixedString(2),
        `DestStateName`                   LowCardinality(String),
        `DestWac`                         Int32,
        `CRSDepTime`                      Int32,
        `DepTime`                         Int32,
        `DepDelay`                        Int32,
        `DepDelayMinutes`                 Int32,
        `DepDel15`                        Int32,
        `DepartureDelayGroups`            LowCardinality(String),
        `DepTimeBlk`                      LowCardinality(String),
        `TaxiOut`                         Int32,
        `WheelsOff`                       LowCardinality(String),
        `WheelsOn`                        LowCardinality(String),
        `TaxiIn`                          Int32,
        `CRSArrTime`                      Int32,
        `ArrTime`                         Int32,
        `ArrDelay`                        Int32,
        `ArrDelayMinutes`                 Int32,
        `ArrDel15`                        Int32,
        `ArrivalDelayGroups`              LowCardinality(String),
        `ArrTimeBlk`                      LowCardinality(String),
        `Cancelled`                       Int8,
        `CancellationCode`                FixedString(1),
        `Diverted`                        Int8,
        `CRSElapsedTime`                  Int32,
        `ActualElapsedTime`               Int32,
        `AirTime`                         Int32,
        `Flights`                         Int32,
        `Distance`                        Int32,
        `DistanceGroup`                   Int8,
        `CarrierDelay`                    Int32,
        `WeatherDelay`                    Int32,
        `NASDelay`                        Int32,
        `SecurityDelay`                   Int32,
        `LateAircraftDelay`               Int32,
        `FirstDepTime`                    Int16,
        `TotalAddGTime`                   Int16,
        `LongestAddGTime`                 Int16,
        `DivAirportLandings`              Int8,
        `DivReachedDest`                  Int8,
        `DivActualElapsedTime`            Int16,
        `DivArrDelay`                     Int16,
        `DivDistance`                     Int16,
        `Div1Airport`                     LowCardinality(String),
        `Div1AirportID`                   Int32,
        `Div1AirportSeqID`                Int32,
        `Div1WheelsOn`                    Int16,
        `Div1TotalGTime`                  Int16,
        `Div1LongestGTime`                Int16,
        `Div1WheelsOff`                   Int16,
        `Div1TailNum`                     LowCardinality(String),
        `Div2Airport`                     LowCardinality(String),
        `Div2AirportID`                   Int32,
        `Div2AirportSeqID`                Int32,
        `Div2WheelsOn`                    Int16,
        `Div2TotalGTime`                  Int16,
        `Div2LongestGTime`                Int16,
        `Div2WheelsOff`                   Int16,
        `Div2TailNum`                     LowCardinality(String),
        `Div3Airport`                     LowCardinality(String),
        `Div3AirportID`                   Int32,
        `Div3AirportSeqID`                Int32,
        `Div3WheelsOn`                    Int16,
        `Div3TotalGTime`                  Int16,
        `Div3LongestGTime`                Int16,
        `Div3WheelsOff`                   Int16,
        `Div3TailNum`                     LowCardinality(String),
        `Div4Airport`                     LowCardinality(String),
        `Div4AirportID`                   Int32,
        `Div4AirportSeqID`                Int32,
        `Div4WheelsOn`                    Int16,
        `Div4TotalGTime`                  Int16,
        `Div4LongestGTime`                Int16,
        `Div4WheelsOff`                   Int16,
        `Div4TailNum`                     LowCardinality(String),
        `Div5Airport`                     LowCardinality(String),
        `Div5AirportID`                   Int32,
        `Div5AirportSeqID`                Int32,
        `Div5WheelsOn`                    Int16,
        `Div5TotalGTime`                  Int16,
        `Div5LongestGTime`                Int16,
        `Div5WheelsOff`                   Int16,
        `Div5TailNum`                     LowCardinality(String)
    ) ENGINE = MergeTree
    ORDER BY (Year, Quarter, Month, DayofMonth, FlightDate, IATA_CODE_Reporting_Airline);

Inserting raw data:   
$ [CLICKHOUSE_EXE] client --query="insert into default.ontime FORMAT CSV" < ./benchmark_sample/rawdata_dir/ontime.csv


Set up database with LZ4 codec for the second [INSTANCE]
``` bash
$ cd ./benchmark_sample/database_dir/lz4_s2
$ [CLICKHOUSE_EXE] server -C config_lz4_s2.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9001
```
Creating a table and Inserting raw data by following the above same steps.

Set up database with with IAA Deflate codec for the first [INSTANCE]
``` bash
$ cd ./benchmark_sample/database_dir/deflate
$ [CLICKHOUSE_EXE] server -C config_deflate.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9000
```
Creating a table and Inserting raw data by following the above same steps.

Set up database with with IAA Deflate codec for the second [INSTANCE]
``` bash
$ cd ./benchmark_sample/database_dir/deflate_s2
$ [CLICKHOUSE_EXE] server -C config_deflate_s2.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9001
```
Creating a table and Inserting raw data by following the above same steps.

Set up database with with ZSTD codec for the first [INSTANCE]
``` bash
$ cd ./benchmark_sample/database_dir/zstd
$ [CLICKHOUSE_EXE] server -C config_zstd.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9000
```
Creating a table and Inserting raw data by following the above same steps.

Set up database with with ZSTD codec for the second [INSTANCE]
``` bash
$ cd ./benchmark_sample/database_dir/zstd_s2
$ [CLICKHOUSE_EXE] server -C config_zstd_s2.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9001
```
Creating a table and Inserting raw data by following the above same steps.

[self-check]
For each codec(lz4/zstd/deflate), please execute below query to make sure the databases are created successfully:
```sql
select count() from ontime
```
You are expected to see below output:
```sql
┌───count()─┐
│ 183953732 │
└───────────┘
```
[Self-check for IAA Deflate codec]
When inserting raw data, clickhouse server log(/tmp/log) is expected to contain below log:
```text
Hardware-assisted DeflateQpl codec is ready!
```
If you never find this, but see other log as below:
```text
Initialization of hardware-assisted DeflateQpl codec failed
```
That means IAA devices is not ready, you need check IAA setup again.

## Run Benchmark

- Before start benchmark, Please make sure only C1 enabled and CPU frequency governor to be `performance`

``` bash
$ cpupower idle-set -e 1
$ cpupower idle-set -d 2
$ cpupower idle-set -d 3
$ cpupower frequency-set -g performance
```

- To eliminate impact of memory bound on cross sockets, we use `numactl` to bind server on one socket and client on another socket.
- The cores of one socket need to be divided equally and assigned to the server [INSTANCE] respectively.Here we assume there are 60 cores per socket and SMT enabled, please customize cores binding according to your working machine.

Now run benchmark for LZ4/Deflate/ZSTD respectively:

LZ4:

``` bash
$ cd ./database_dir/lz4
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_lz4.xml >&/dev/null&
$ cd ./database_dir/lz4_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_lz4_s2.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ontime.sql 2  > lz4_2insts.log
```

ZSTD:

``` bash
$ cd ./database_dir/zstd
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
$ cd ./database_dir/zstd_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_zstd_s2.xml >&/dev/null& 
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ontime.sql 2 > zstd_2insts.log
```

IAA deflate

``` bash
$ cd ./database_dir/deflate
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
$ cd ./database_dir/deflate_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_deflate_s2.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ontime.sql 2 > deflate_2insts.log
```

Now three logs should be output as expected:

``` text
lz4_2insts.log
deflate_2insts.log
zstd_2insts.log
```
How to check performance metrics:

We focus on QPS(Query Per Second) to compare performance data for LZ4/Deflate/ZSTD, please search the keyword: `QPS_Final` to collect statistics.

## Tips

Each time before relaunch clickhouse server [INSTANCE], please make sure no same [INSTANCE] running on background, please check and kill old one.