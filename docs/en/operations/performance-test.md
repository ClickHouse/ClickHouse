---
toc_priority: 54
toc_title: Testing Hardware
---

# How to Test Your Hardware with ClickHouse {#how-to-test-your-hardware-with-clickhouse}

You can run basic ClickHouse performance test on any server without installation of ClickHouse packages.


## Automated Run

You can run benchmark with a single script.

1. Download the script.
```
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/hardware.sh
```

2. Run the script.
```
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copy the output and send it to clickhouse-feedback@yandex-team.com

All the results are published here: https://clickhouse.com/benchmark/hardware/


## Manual Run

Alternatively you can perform benchmark in the following steps.

1.  ssh to the server and download the binary with wget:
```bash
# For amd64:
wget https://builds.clickhouse.com/master/amd64/clickhouse
# For aarch64:
wget https://builds.clickhouse.com/master/aarch64/clickhouse
# Then do:
chmod a+x clickhouse
```
2.  Download benchmark files:
```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
chmod a+x benchmark-new.sh
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql
```
3.  Download test data according to the [Yandex.Metrica dataset](../getting-started/example-datasets/metrica.md) instruction (“hits” table containing 100 million rows).
```bash
wget https://datasets.clickhouse.com/hits/partitions/hits_100m_obfuscated_v1.tar.xz
tar xvf hits_100m_obfuscated_v1.tar.xz -C .
mv hits_100m_obfuscated_v1/* .
```
4.  Run the server:
```bash
./clickhouse server
```
5.  Check the data: ssh to the server in another terminal
```bash
./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
100000000
```
6.  Run the benchmark:
```bash
./benchmark-new.sh hits_100m_obfuscated
```
7.  Send the numbers and the info about your hardware configuration to clickhouse-feedback@yandex-team.com

All the results are published here: https://clickhouse.com/benchmark/hardware/
