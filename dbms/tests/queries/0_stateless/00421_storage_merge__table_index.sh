#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for i in `seq -w 0 2 20`; do
    clickhouse-client -q "DROP TABLE IF EXISTS test.merge_item_$i"
    clickhouse-client -q "CREATE TABLE test.merge_item_$i (d Int8) ENGINE = Memory"
    clickhouse-client -q "INSERT INTO test.merge_item_$i VALUES ($i)"
done

clickhouse-client -q "DROP TABLE IF EXISTS test.merge_storage"
clickhouse-client -q "CREATE TABLE test.merge_storage (d Int8) ENGINE = Merge('test', '^merge_item_')"
clickhouse-client --max_threads=1 -q "SELECT _table, d FROM test.merge_storage WHERE _table LIKE 'merge_item_1%' ORDER BY _table"
clickhouse-client -q "DROP TABLE IF EXISTS test.merge_storage"

for i in `seq -w 0 2 20`; do clickhouse-client -q "DROP TABLE IF EXISTS test.merge_item_$i"; done
