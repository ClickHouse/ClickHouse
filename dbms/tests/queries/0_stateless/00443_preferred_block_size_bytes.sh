#!/bin/bash
set -e

clickhouse-client -q "DROP TABLE IF EXISTS test.preferred_block_size_bytes"
clickhouse-client -q "CREATE TABLE test.preferred_block_size_bytes (p Date, s String) ENGINE = MergeTree(p, p, 1)"
clickhouse-client -q "INSERT INTO test.preferred_block_size_bytes (s) SELECT '16_bytes_-_-_-_' AS s FROM system.numbers LIMIT 10, 90"
clickhouse-client -q "OPTIMIZE TABLE test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=26 -q "SELECT DISTINCT blockSize(), ignore(p, s) FROM test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=52 -q "SELECT DISTINCT blockSize(), ignore(p, s) FROM test.preferred_block_size_bytes"
clickhouse-client --preferred_block_size_bytes=90 -q "SELECT DISTINCT blockSize(), ignore(p) FROM test.preferred_block_size_bytes"
clickhouse-client -q "DROP TABLE IF EXISTS test.preferred_block_size_bytes"

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
