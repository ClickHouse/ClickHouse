#!/usr/bin/env bash
set -e

clickhouse-client -q "DROP TABLE IF EXISTS test.preferred_block_size_bytes"
clickhouse-client -q "CREATE TABLE test.preferred_block_size_bytes (p Date, s String) ENGINE = MergeTree(p, p, 1)"
clickhouse-client -q "INSERT INTO test.preferred_block_size_bytes (s) SELECT '16_bytes_-_-_-_' AS s FROM system.numbers LIMIT 10, 90"
clickhouse-client -q "OPTIMIZE TABLE test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=26 -q "SELECT DISTINCT blockSize(), ignore(p, s) FROM test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=52 -q "SELECT DISTINCT blockSize(), ignore(p, s) FROM test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=90 -q "SELECT DISTINCT blockSize(), ignore(p) FROM test.preferred_block_size_bytes"
clickhouse-client -q "DROP TABLE IF EXISTS test.preferred_block_size_bytes"

# PREWHERE using empty column

clickhouse-client -q "DROP TABLE IF EXISTS test.pbs"
clickhouse-client -q "CREATE TABLE test.pbs (p Date, i UInt64, sa Array(String)) ENGINE = MergeTree(p, p, 100)"
clickhouse-client -q "INSERT INTO test.pbs (p, i, sa) SELECT toDate(i % 30) AS p, number AS i, ['a'] AS sa FROM system.numbers LIMIT 1000"
clickhouse-client -q "ALTER TABLE test.pbs ADD COLUMN s UInt8 DEFAULT 0"
clickhouse-client --preferred_block_size_bytes=100000 -q "SELECT count() FROM test.pbs PREWHERE s = 0"
clickhouse-client -q "INSERT INTO test.pbs (p, i, sa) SELECT toDate(i % 30) AS p, number AS i, ['a'] AS sa FROM system.numbers LIMIT 1000"
clickhouse-client --preferred_block_size_bytes=100000 -q "SELECT count() FROM test.pbs PREWHERE s = 0"
clickhouse-client -q "DROP TABLE test.pbs"

# Nullable PREWHERE

clickhouse-client -q "DROP TABLE IF EXISTS test.nullable_prewhere"
clickhouse-client -q "CREATE TABLE test.nullable_prewhere (p Date, f Nullable(UInt64), d UInt64) ENGINE = MergeTree(p, p, 8)"
clickhouse-client -q "INSERT INTO test.nullable_prewhere SELECT toDate(0) AS p, if(number % 2 = 0, CAST(number AS Nullable(UInt64)), CAST(NULL AS Nullable(UInt64))) AS f, number as d FROM system.numbers LIMIT 1001"
clickhouse-client -q "SELECT sum(d), sum(f), max(d) FROM test.nullable_prewhere PREWHERE NOT isNull(f)"
clickhouse-client -q "DROP TABLE IF EXISTS test.nullable_prewhere"

# Depend on 00282_merging test

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null
#SCRIPTDIR=`dirname "$SCRIPTPATH"`
SCRIPTDIR=$SCRIPTPATH

cat "$SCRIPTDIR"/00282_merging.sql | clickhouse-client --preferred_block_size_bytes=10 --merge_tree_uniform_read_distribution=1 -n 2>&1 > preferred_block_size_bytes.stdout
cmp "$SCRIPTDIR"/00282_merging.reference preferred_block_size_bytes.stdout && echo PASSED || echo FAILED

cat "$SCRIPTDIR"/00282_merging.sql | clickhouse-client --preferred_block_size_bytes=20 --merge_tree_uniform_read_distribution=0 -n 2>&1 > preferred_block_size_bytes.stdout
cmp "$SCRIPTDIR"/00282_merging.reference preferred_block_size_bytes.stdout && echo PASSED || echo FAILED

rm preferred_block_size_bytes.stdout
