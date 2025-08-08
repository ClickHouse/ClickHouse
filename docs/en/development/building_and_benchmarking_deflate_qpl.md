---
description: 'How to build Clickhouse and run benchmark with DEFLATE_QPL Codec'
sidebar_label: 'Building and Benchmarking DEFLATE_QPL'
sidebar_position: 73
slug: /development/building_and_benchmarking_deflate_qpl
title: 'Build Clickhouse with DEFLATE_QPL'
---

# Build Clickhouse with DEFLATE_QPL

- Make sure your host machine meet the QPL required [prerequisites](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#prerequisites)
- deflate_qpl is enabled by default during cmake build. In case you accidentally change it, please double-check build flag: ENABLE_QPL=1

- For generic requirements, please refer to Clickhouse generic [build instructions](/development/build.md)

# Run Benchmark with DEFLATE_QPL

## Files list {#files-list}

The folders `benchmark_sample` under [qpl-cmake](https://github.com/ClickHouse/ClickHouse/tree/master/contrib/qpl-cmake) give example to run benchmark with python scripts:

`client_scripts` contains python scripts for running typical benchmark, for example:
- `client_stressing_test.py`: The python script for query stress test with [1~4] server instances.
- `queries_ssb.sql`: The file lists all queries for [Star Schema Benchmark](/getting-started/example-datasets/star-schema/)
- `allin1_ssb.sh`: This shell script executes benchmark workflow all in one automatically.

`database_files` means it will store database files according to lz4/deflate/zstd codec.

## Run benchmark automatically for Star Schema: {#run-benchmark-automatically-for-star-schema}

```bash
$ cd ./benchmark_sample/client_scripts
$ sh run_ssb.sh
```

After complete, please check all the results in this folder:`./output/`

In case you run into failure, please manually run benchmark as below sections.

## Definition {#definition}

[CLICKHOUSE_EXE] means the path of clickhouse executable program.

## Environment {#environment}

- CPU: Sapphire Rapid
- OS Requirements refer to [System Requirements for QPL](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#system-requirements)
- IAA Setup refer to [Accelerator Configuration](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#accelerator-configuration)
- Install python modules:

```bash
pip3 install clickhouse_driver numpy
```

[Self-check for IAA]

```bash
$ accel-config list | grep -P 'iax|state'
```

Expected output like this:
```bash
    "dev":"iax1",
    "state":"enabled",
            "state":"enabled",
```

If you see nothing output, it means IAA is not ready to work. Please check IAA setup again.

## Generate raw data {#generate-raw-data}

```bash
$ cd ./benchmark_sample
$ mkdir rawdata_dir && cd rawdata_dir
```

Use [`dbgen`](/getting-started/example-datasets/star-schema) to generate 100 million rows data with the parameters:
-s 20

The files like `*.tbl` are expected to output under `./benchmark_sample/rawdata_dir/ssb-dbgen`:

## Database setup {#database-setup}

Set up database with LZ4 codec

```bash
$ cd ./database_dir/lz4
$ [CLICKHOUSE_EXE] server -C config_lz4.xml >&/dev/null&
$ [CLICKHOUSE_EXE] client
```

Here you should see the message `Connected to ClickHouse server` from console which means client successfully setup connection with server.

Complete below three steps mentioned in [Star Schema Benchmark](/getting-started/example-datasets/star-schema)
- Creating tables in ClickHouse
- Inserting data. Here should use `./benchmark_sample/rawdata_dir/ssb-dbgen/*.tbl` as input data.
- Converting "star schema" to de-normalized "flat schema"

Set up database with IAA Deflate codec

```bash
$ cd ./database_dir/deflate
$ [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
$ [CLICKHOUSE_EXE] client
```
Complete three steps same as lz4 above

Set up database with ZSTD codec

```bash
$ cd ./database_dir/zstd
$ [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
$ [CLICKHOUSE_EXE] client
```
Complete three steps same as lz4 above

[self-check]
For each codec(lz4/zstd/deflate), please execute below query to make sure the databases are created successfully:
```sql
SELECT count() FROM lineorder_flat
```
You are expected to see below output:
```sql
┌───count()─┐
│ 119994608 │
└───────────┘
```
[Self-check for IAA Deflate codec]

At the first time you execute insertion or query from client, clickhouse server console is expected to print this log:
```text
Hardware-assisted DeflateQpl codec is ready!
```
If you never find this, but see another log as below:
```text
Initialization of hardware-assisted DeflateQpl codec failed
```
That means IAA devices is not ready, you need check IAA setup again.

## Benchmark with single instance {#benchmark-with-single-instance}

- Before start benchmark, Please disable C6 and set CPU frequency governor to be `performance`

```bash
$ cpupower idle-set -d 3
$ cpupower frequency-set -g performance
```

- To eliminate impact of memory bound on cross sockets, we use `numactl` to bind server on one socket and client on another socket.
- Single instance means single server connected with single client

Now run benchmark for LZ4/Deflate/ZSTD respectively:

LZ4:

```bash
$ cd ./database_dir/lz4 
$ numactl -m 0 -N 0 [CLICKHOUSE_EXE] server -C config_lz4.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 1 > lz4.log
```

IAA deflate:

```bash
$ cd ./database_dir/deflate
$ numactl -m 0 -N 0 [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 1 > deflate.log
```

ZSTD:

```bash
$ cd ./database_dir/zstd
$ numactl -m 0 -N 0 [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 1 > zstd.log
```

Now three logs should be output as expected:
```text
lz4.log
deflate.log
zstd.log
```

How to check performance metrics:

We focus on QPS, please search the keyword: `QPS_Final` and collect statistics

## Benchmark with multi-instances {#benchmark-with-multi-instances}

- To reduce impact of memory bound on too much threads, We recommend run benchmark with multi-instances.
- Multi-instance means multiple（2 or 4）servers connected with respective client.
- The cores of one socket need to be divided equally and assigned to the servers respectively.
- For multi-instances, must create new folder for each codec and insert dataset by following the similar steps as single instance.

There are 2 differences: 
- For client side, you need launch clickhouse with the assigned port during table creation and data insertion.
- For server side, you need launch clickhouse with the specific xml config file in which port has been assigned. All customized xml config files for multi-instances has been provided under ./server_config.

Here we assume there are 60 cores per socket and take 2 instances for example.
Launch server for first instance
LZ4:

```bash
$ cd ./database_dir/lz4
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_lz4.xml >&/dev/null&
```

ZSTD:

```bash
$ cd ./database_dir/zstd
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
```

IAA Deflate:

```bash
$ cd ./database_dir/deflate
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
```

[Launch server for second instance]

LZ4:

```bash
$ cd ./database_dir && mkdir lz4_s2 && cd lz4_s2
$ cp ../../server_config/config_lz4_s2.xml ./
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_lz4_s2.xml >&/dev/null&
```

ZSTD:

```bash
$ cd ./database_dir && mkdir zstd_s2 && cd zstd_s2
$ cp ../../server_config/config_zstd_s2.xml ./
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_zstd_s2.xml >&/dev/null&
```

IAA Deflate:

```bash
$ cd ./database_dir && mkdir deflate_s2 && cd deflate_s2
$ cp ../../server_config/config_deflate_s2.xml ./
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_deflate_s2.xml >&/dev/null&
```

Creating tables && Inserting data for second instance

Creating tables:

```bash
$ [CLICKHOUSE_EXE] client -m --port=9001 
```

Inserting data:

```bash
$ [CLICKHOUSE_EXE] client --query "INSERT INTO [TBL_FILE_NAME] FORMAT CSV" < [TBL_FILE_NAME].tbl  --port=9001
```

- [TBL_FILE_NAME] represents the name of a file named with the regular expression: *. tbl under `./benchmark_sample/rawdata_dir/ssb-dbgen`.
- `--port=9001` stands for the assigned port for server instance which is also defined in config_lz4_s2.xml/config_zstd_s2.xml/config_deflate_s2.xml. For even more instances, you need replace it with the value: 9002/9003 which stand for s3/s4 instance respectively. If you don't assign it, the port is 9000 by default which has been used by first instance.

Benchmarking with 2 instances

LZ4:

```bash
$ cd ./database_dir/lz4
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_lz4.xml >&/dev/null&
$ cd ./database_dir/lz4_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_lz4_s2.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2  > lz4_2insts.log
```

ZSTD:

```bash
$ cd ./database_dir/zstd
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_zstd.xml >&/dev/null&
$ cd ./database_dir/zstd_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_zstd_s2.xml >&/dev/null& 
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2 > zstd_2insts.log
```

IAA deflate

```bash
$ cd ./database_dir/deflate
$ numactl -C 0-29,120-149 [CLICKHOUSE_EXE] server -C config_deflate.xml >&/dev/null&
$ cd ./database_dir/deflate_s2
$ numactl -C 30-59,150-179 [CLICKHOUSE_EXE] server -C config_deflate_s2.xml >&/dev/null&
$ cd ./client_scripts
$ numactl -m 1 -N 1 python3 client_stressing_test.py queries_ssb.sql 2 > deflate_2insts.log
```

Here the last argument: `2` of client_stressing_test.py stands for the number of instances. For more instances, you need replace it with the value: 3 or 4. This script support up to 4 instances/

Now three logs should be output as expected:

```text
lz4_2insts.log
deflate_2insts.log
zstd_2insts.log
```
How to check performance metrics:

We focus on QPS, please search the keyword: `QPS_Final` and collect statistics

Benchmark setup for 4 instances is similar with 2 instances above.
We recommend use 2 instances benchmark data as final report for review.

## Tips {#tips}

Each time before launch new clickhouse server, please make sure no background clickhouse process running, please check and kill old one:

```bash
$ ps -aux| grep clickhouse
$ kill -9 [PID]
```
By comparing the query list in ./client_scripts/queries_ssb.sql with official [Star Schema Benchmark](/getting-started/example-datasets/star-schema), you will find 3 queries are not included: Q1.2/Q1.3/Q3.4 . This is because cpu utilization% is very low < 10% for these queries which means cannot demonstrate performance differences.
