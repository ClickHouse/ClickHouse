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
- `queries_ssb.sql`: The file lists all queries for [Star Schema Benchmark](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema/)
- `allin1_ssb.sh`: This shell script executes benchmark workflow all in one automatically.

`database_files` means it will store database files according to lz4/deflate/zstd codec.


## Run benchmark automatically:

``` bash
$ cd ./benchmark_sample/client_scripts
$ sh run_ssb.sh
```

After complete, please check all the results in this folder:`./output/`
In case you run into failure, please manually run benchmark as below sections.

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
$ cd ./benchmark_sample
$ mkdir rawdata_dir && cd rawdata_dir
```

Use [`dbgen`](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema) to generate 100 million rows data with the parameters:
-s 20

The files like `*.tbl` are expected to output under `./benchmark_sample/rawdata_dir/ssb-dbgen`:
```

## Database setup

Set up database with LZ4 codec for the first [INSTANCE]

``` bash
$ cd ./benchmark_sample/database_dir/lz4
$ [CLICKHOUSE_EXE] server -C config_lz4.xml >&/tmp/log&
$ [CLICKHOUSE_EXE] client -m --port=9000
```

Here you should see the message `Connected to ClickHouse server` from console which means client successfully setup connection with server.

Creating a table and Inserting raw data as below three steps, for details please refer to [Star Schema Benchmark](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema)
- Creating tables in ClickHouse
- Inserting data. Here should use `./benchmark_sample/rawdata_dir/ssb-dbgen/*.tbl` as input data.
- Converting “star schema” to de-normalized “flat schema”

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
select count() from lineorder_flat
```
You are expected to see below output:
```sql
┌───count()─┐
│ 119994608  │
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
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2  > lz4_2insts.log
```

ZSTD:

``` bash
$ cd ./database_dir/zstd
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
$ cd ./database_dir/zstd_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_zstd_s2.xml >&/dev/null& 
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2 > zstd_2insts.log
```

IAA deflate

``` bash
$ cd ./database_dir/deflate
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
$ cd ./database_dir/deflate_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_deflate_s2.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2 > deflate_2insts.log
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

- Each time before relaunch clickhouse server [INSTANCE], please make sure no same [INSTANCE] running on background, please check and kill old one.
- By comparing the query list in ./client_scripts/queries_ssb.sql with official [Star Schema Benchmark](https://clickhouse.com/docs/en/getting-started/example-datasets/star-schema), you will find three queries are not included: Q1.2/Q1.3/Q3.4 . This is because cpu utilization% is very low <10% for these queries which means cannot demonstrate performance differences.
