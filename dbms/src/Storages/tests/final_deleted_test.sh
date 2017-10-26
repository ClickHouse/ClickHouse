#!/usr/bin/env bash

echo "DROP DATABASE IF EXISTS final_deleted_test" | clickhouse-client || exit 1
echo "CREATE DATABASE final_deleted_test" | clickhouse-client || exit 2
echo "CREATE TABLE final_deleted_test.a ( d Date, k String, s Int8, v String) ENGINE = CollapsingMergeTree(d, tuple(k), 8192, s)" | clickhouse-client || exit 4

echo "('2014-01-01','key1',1,'val1'),('2014-01-01','key1',-1,'val1')" | clickhouse-client --query="INSERT INTO final_deleted_test.a VALUES" || exit 5

echo "('2014-02-02','key2',-1,'val2'),('2014-02-02','key2',1,'val3'),('2014-02-02','key2',-1,'val3')" | clickhouse-client --query="INSERT INTO final_deleted_test.a VALUES" || exit 8

echo "SELECT * FROM final_deleted_test.a" | clickhouse-client | tee /tmp/t1 || exit 10
echo
echo "SELECT * FROM final_deleted_test.a FINAL" | clickhouse-client | tee /tmp/t2 || exit 9

f=0

if [ `cat /tmp/t1 | wc -l` -ne 5 ]
then
    echo 'Failed 1'
    f=1
fi

if [ `cat /tmp/t2 | wc -l` -ne 0 ]
then
    echo 'Failed 2'
    f=1
fi

if [ $f -eq 0 ]
then
    echo 'Passed'
fi

