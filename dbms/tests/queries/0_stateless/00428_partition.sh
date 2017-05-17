#!/bin/bash
set -e
# Not found column date in block. There are only columns: x.

# Test 1. Complex test checking columns.txt

chl="clickhouse-client -q"
ch_dir=`clickhouse --extract-from-config -c /etc/clickhouse-server/config.xml -k path`

$chl "DROP TABLE IF EXISTS test.partition_428"
$chl "CREATE TABLE test.partition_428 (p Date, k Int8, v1 Int8 MATERIALIZED k + 1) ENGINE = MergeTree(p, k, 1)"
$chl "INSERT INTO test.partition_428 (p, k) VALUES(toDate(31), 1)"
$chl "INSERT INTO test.partition_428 (p, k) VALUES(toDate(1), 2)"

for part in `$chl "SELECT name FROM system.parts WHERE database='test' AND table='partition_428'"`; do
    cat $ch_dir/data/test/partition_428/$part/columns.txt | wc -l # 2 header lines + 3 columns
done

$chl "ALTER TABLE test.partition_428 DETACH PARTITION 197001"
$chl "ALTER TABLE test.partition_428 ATTACH PARTITION 197001"

for part in `$chl "SELECT name FROM system.parts WHERE database='test' AND table='partition_428'"`; do
    cat $ch_dir/data/test/partition_428/$part/columns.txt | wc -l # 2 header lines + 3 columns
done

$chl "ALTER TABLE test.partition_428 MODIFY COLUMN v1 Int8"
$chl "OPTIMIZE TABLE test.partition_428"

$chl "SELECT toUInt16(p), k, v1 FROM test.partition_428 ORDER BY k FORMAT CSV"
$chl "DROP TABLE test.partition_428"

# Test 2. Simple test

$chl "drop table if exists test.partition_428"
$chl "create table test.partition_428 (date MATERIALIZED toDate(0), x UInt64, sample_key MATERIALIZED intHash64(x)) ENGINE=MergeTree(date,sample_key,(date,x,sample_key),8192)"
$chl "insert into test.partition_428 ( x ) VALUES ( now() )"
$chl "insert into test.partition_428 ( x ) VALUES ( now()+1 )"
$chl "alter table test.partition_428 detach partition 197001"
$chl "alter table test.partition_428 attach partition 197001"
$chl "optimize table test.partition_428"
$chl "drop table test.partition_428"
