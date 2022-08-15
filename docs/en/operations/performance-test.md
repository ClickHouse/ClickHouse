---
toc_priority: 54
toc_title: Testing Hardware
---

# How to Test Your Hardware with ClickHouse {#how-to-test-your-hardware-with-clickhouse}

With this instruction you can run basic ClickHouse performance test on any server without installation of ClickHouse packages.

1.  Go to “commits” page: https://github.com/ClickHouse/ClickHouse/commits/master
2.  Click on the first green check mark or red cross with green “ClickHouse Build Check” and click on the “Details” link near “ClickHouse Build Check”. There is no such link in some commits, for example commits with documentation. In this case, choose the nearest commit having this link.
3.  Copy the link to `clickhouse` binary for amd64 or aarch64.
4.  ssh to the server and download it with wget:
```bash
# These links are outdated, please obtain the fresh link from the "commits" page.
# For amd64:
wget https://clickhouse-builds.s3.yandex.net/0/e29c4c3cc47ab2a6c4516486c1b77d57e7d42643/clickhouse_build_check/gcc-10_relwithdebuginfo_none_bundled_unsplitted_disable_False_binary/clickhouse
# For aarch64:
wget https://clickhouse-builds.s3.yandex.net/0/e29c4c3cc47ab2a6c4516486c1b77d57e7d42643/clickhouse_special_build_check/clang-10-aarch64_relwithdebuginfo_none_bundled_unsplitted_disable_False_binary/clickhouse
# Then do:
chmod a+x clickhouse
```
5.  Download benchmark files:
```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
chmod a+x benchmark-new.sh
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql
```
6.  Download test data according to the [Yandex.Metrica dataset](../getting-started/example-datasets/metrica.md) instruction (“hits” table containing 100 million rows).
```bash
wget https://datasets.clickhouse.tech/hits/partitions/hits_100m_obfuscated_v1.tar.xz
tar xvf hits_100m_obfuscated_v1.tar.xz -C .
mv hits_100m_obfuscated_v1/* .
```
7.  Run the server:
```bash
./clickhouse server
```
8.  Check the data: ssh to the server in another terminal
```bash
./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
100000000
```
9.  Edit the benchmark-new.sh, change `clickhouse-client` to `./clickhouse client` and add `--max_memory_usage 100000000000` parameter.
```bash
mcedit benchmark-new.sh
```
10.  Run the benchmark:
```bash
./benchmark-new.sh hits_100m_obfuscated
```
11.  Send the numbers and the info about your hardware configuration to clickhouse-feedback@yandex-team.com

All the results are published here: https://clickhouse.tech/benchmark/hardware/
