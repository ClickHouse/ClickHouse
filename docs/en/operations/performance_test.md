# How To Test Your Hardware With ClickHouse

Draft.

With this instruction you can run basic ClickHouse performance test on any server without installation of ClickHouse packages.

1. Go to "commits" page: https://github.com/ClickHouse/ClickHouse/commits/master

2. Click on the first green check mark or red cross with green "ClickHouse Build Check" and click on the "Details" link near "ClickHouse Build Check".

3. Copy the link to "clickhouse" binary for amd64 or aarch64.

4. ssh to the server and download it with wget:
```
# For amd64:
wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
# For aarch64:
wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
# Then do:
chmod a+x clickhouse
```

5. Download configs:
```
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/programs/server/config.xml
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/programs/server/users.xml
mkdir config.d
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/programs/server/config.d/path.xml -O config.d/path.xml
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml
```

6. Download benchmark files:
```
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/benchmark/clickhouse/benchmark-new.sh
chmod a+x benchmark-new.sh
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/benchmark/clickhouse/queries.sql
```

7. Download test data:
According to the instruction:
https://clickhouse.yandex/docs/en/getting_started/example_datasets/metrica/
("hits" table containing 100 million rows)

```
wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
tar xvf hits_100m_obfuscated_v1.tar.xz -C .
mv hits_100m_obfuscated_v1/* .
```

8. Run the server:
```
./clickhouse server
```

9. Check the data:
ssh to the server in another terminal
```
./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
100000000
```

10. Edit the benchmark-new.sh, change "clickhouse-client" to "./clickhouse client" and add "--max_memory_usage 100000000000" parameter.
```
mcedit benchmark-new.sh
```

11. Run the benchmark:
```
./benchmark-new.sh hits_100m_obfuscated
```

12. Send the numbers and the info about your hardware configuration to clickhouse-feedback@yandex-team.com
