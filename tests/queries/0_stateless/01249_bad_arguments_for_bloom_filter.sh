#!/usr/bin/env bash
# Tags: no-parallel, no-flaky-check

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Creation of a database with Ordinary engine emits a warning.
$CLICKHOUSE_CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "DROP DATABASE IF EXISTS test_01249; CREATE DATABASE test_01249 ENGINE = Ordinary"

columns='`u64` UInt64, `i32` Int32, `f64` Float64, `d` Decimal(10, 2), `s` String, `e` Enum8('\''a'\'' = 1, '\''b'\'' = 2, '\''c'\'' = 3), `dt` Date'
storage='ENGINE = MergeTree() ORDER BY u64 SETTINGS index_granularity = 8192'

# A new definition with bad arguments is rejected, also by a full-definition ATTACH.
for statement in CREATE ATTACH; do
    $CLICKHOUSE_CLIENT -q "$statement TABLE test_01249.bloom_filter_idx_good($columns, INDEX bloom_filter_a i32 TYPE bloom_filter(0, 1) GRANULARITY 1) $storage; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }"
    $CLICKHOUSE_CLIENT -q "$statement TABLE test_01249.bloom_filter_idx_good($columns, INDEX bloom_filter_a i32 TYPE bloom_filter(-0.1) GRANULARITY 1) $storage; -- { serverError BAD_ARGUMENTS }"
    $CLICKHOUSE_CLIENT -q "$statement TABLE test_01249.bloom_filter_idx_good($columns, INDEX bloom_filter_a i32 TYPE bloom_filter(1.01) GRANULARITY 1) $storage; -- { serverError BAD_ARGUMENTS }"
done

# A stored definition with bad arguments still loads: it is rewritten into the stored metadata of a
# valid table, which is then attached back with the short ATTACH.
for arguments in '0., 1.' '-0.1' '1.01'; do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE test_01249.bloom_filter_idx_good($columns, INDEX bloom_filter_a i32 TYPE bloom_filter(0.025) GRANULARITY 1) $storage"
    metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = 'test_01249' AND name = 'bloom_filter_idx_good'")
    $CLICKHOUSE_CLIENT -q "DETACH TABLE test_01249.bloom_filter_idx_good"
    sed -i.bak "s/bloom_filter(0.025)/bloom_filter($arguments)/" "$metadata_file" && rm -f "$metadata_file.bak"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE test_01249.bloom_filter_idx_good"
    $CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE test_01249.bloom_filter_idx_good"
    $CLICKHOUSE_CLIENT -q "DROP TABLE test_01249.bloom_filter_idx_good"
done

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01249"
