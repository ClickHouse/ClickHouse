#!/usr/bin/env bash

clickhouse-client -n --query="
    DROP TABLE IF EXISTS test.dst;
    DROP TABLE IF EXISTS test.buffer;

    CREATE TABLE test.dst (x UInt64, d Date DEFAULT today()) ENGINE = MergeTree(d, x, 8192);
    CREATE TABLE test.buffer (x UInt64, d Date DEFAULT today()) ENGINE = Buffer(test, dst, 16, 1, 10, 100, 1000, 10000, 100000);
    ";

seq    1 1000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 1001 2000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 2001 3000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 3001 4000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 4001 5000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 5001 6000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 6001 7000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 7001 8000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 8001 9000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 9001 10000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 10001 11000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 11001 12000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 12001 13000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 13001 14000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 14001 15000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 15001 16000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 16001 17000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 17001 18000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 18001 19000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &
seq 19001 20000 | sed -r -e 's/^(.+)$/INSERT INTO test.buffer (x) VALUES (\1);/' | clickhouse-client -n &

wait

# To avoid race conditions (see comments in StorageBuffer.cpp)
function retry()
{
    RES=$(clickhouse-client --query="$1")
    if [[ $RES != "20000    1    20000    200010000    20000" ]]; then
        sleep 10;
        RES=$(clickhouse-client --query="$1");
    fi;

    echo $RES;
}

retry "SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.buffer;";
clickhouse-client --query="OPTIMIZE TABLE test.buffer;";
retry "SELECT count(), min(x), max(x), sum(x), uniqExact(x) FROM test.dst;";

clickhouse-client -n --query="
    DROP TABLE test.dst;
    DROP TABLE test.buffer;
    ";
