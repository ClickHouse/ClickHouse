#!/usr/bin/env bash

# METR-9072

echo "DROP DATABASE IF EXISTS collapsing_test" | clickhouse-client || exit 1
echo "CREATE DATABASE collapsing_test" | clickhouse-client || exit 2
echo "CREATE TABLE collapsing_test.p0 ( d Date, k String,  s Int8,  v String) ENGINE = CollapsingMergeTree(d, tuple(k), 8192, s)" | clickhouse-client || exit 3
echo "CREATE TABLE collapsing_test.p1 AS collapsing_test.p0" | clickhouse-client || exit 4
echo "CREATE TABLE collapsing_test.p2 AS collapsing_test.p0" | clickhouse-client || exit 5
echo "CREATE TABLE collapsing_test.m0 AS collapsing_test.p0" | clickhouse-client || exit 9
echo "CREATE TABLE collapsing_test.m1 AS collapsing_test.p0" | clickhouse-client || exit 11
echo "('2014-01-01', 'key1', 1, 'val1')" | clickhouse-client --query="INSERT INTO collapsing_test.p0 VALUES" || exit 6
echo "('2014-01-01', 'key1', -1, 'val1'),('2014-01-01', 'key1', 1, 'val2')" | clickhouse-client --query="INSERT INTO collapsing_test.p1 VALUES" || exit 7
echo "('2014-01-01', 'key1', -1, 'val2')" | clickhouse-client --query="INSERT INTO collapsing_test.p2 VALUES" || exit 8

sudo /etc/init.d/clickhouse-server stop || exit 10

sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p0/20140101_20140101_1_1_0,m0/} || exit 12
sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p1/20140101_20140101_1_1_0,m0/20140101_20140101_2_2_0} || exit 13
sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p1/20140101_20140101_1_1_0,m1/20140101_20140101_2_2_0} || exit 14
sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p2/20140101_20140101_1_1_0,m1/20140101_20140101_3_3_0} || exit 15
rm /opt/clickhouse/data/collapsing_test/m{0,1}/increment.txt || exit 29

sudo /etc/init.d/clickhouse-server start || exit 16

sleep 10s
echo "OPTIMIZE TABLE collapsing_test.m0" | clickhouse-client || exit 17
echo "OPTIMIZE TABLE collapsing_test.m1" | clickhouse-client || exit 18

sudo /etc/init.d/clickhouse-server stop || exit 19

sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p0/20140101_20140101_1_1_0,m1/} || exit 20
sudo -u metrika cp -r /opt/clickhouse/data/collapsing_test/{p2/20140101_20140101_1_1_0,m0/20140101_20140101_3_3_0} || exit 21
rm /opt/clickhouse/data/collapsing_test/m{0,1}/increment.txt || exit 29

sudo /etc/init.d/clickhouse-server start || exit 22

sleep 10s
echo "OPTIMIZE TABLE collapsing_test.m0" | clickhouse-client || exit 23
echo "OPTIMIZE TABLE collapsing_test.m1" | clickhouse-client || exit 23

ls /opt/clickhouse/data/collapsing_test/m{0,1}

echo "SELECT * FROM collapsing_test.m0" | clickhouse-client || exit 24
echo
echo "SELECT * FROM collapsing_test.m1" | clickhouse-client || exit 25
echo

echo "('2014-01-01', 'key2', 1, 'val')" | clickhouse-client --query="INSERT INTO collapsing_test.m0 VALUES" || exit 33
echo "('2014-01-01', 'key2', 1, 'val')" | clickhouse-client --query="INSERT INTO collapsing_test.m1 VALUES" || exit 32
echo "OPTIMIZE TABLE collapsing_test.m0" | clickhouse-client || exit 30
echo "OPTIMIZE TABLE collapsing_test.m1" | clickhouse-client || exit 31

ls /opt/clickhouse/data/collapsing_test/m{0,1}

echo "SELECT * FROM collapsing_test.m0" | clickhouse-client | tee /tmp/t1 || exit 24
echo
echo "SELECT * FROM collapsing_test.m1" | clickhouse-client | tee /tmp/t2 || exit 25

diff -q /tmp/t{1,2}
if [ $? -ne 0 ]
then
	echo 'Failed'
	exit 27
else
	echo 'Passed'
fi

