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

# A stored definition with bad arguments still loads. It is emulated in a clickhouse-local data directory:
# the stored definition of a valid table is rewritten, and the next start loads it.
$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/01249_data" -q "CREATE DATABASE test_01249"
for arguments in '0., 1.' '-0.1' '1.01'; do
    $CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/01249_data" -q "CREATE TABLE test_01249.bloom_filter_idx_good($columns, INDEX bloom_filter_a i32 TYPE bloom_filter(0.025) GRANULARITY 1) $storage"
    $CLICKHOUSE_LOCAL --param_metadata="${CLICKHOUSE_TMP:?}/01249_data/metadata/test_01249/bloom_filter_idx_good.sql" --param_copy="${CLICKHOUSE_TMP:?}/01249_data.sql" --param_arguments="$arguments" -q "
        SET engine_file_truncate_on_insert = 1;
        INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
            SELECT replaceOne(raw_blob, 'bloom_filter(0.025)', 'bloom_filter(' || {arguments:String} || ')') FROM file({metadata:String}, 'RawBLOB');
        INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
    $CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/01249_data" -q "SHOW CREATE TABLE test_01249.bloom_filter_idx_good; DROP TABLE test_01249.bloom_filter_idx_good SYNC"
done
rm -rf "${CLICKHOUSE_TMP:?}/01249_data" "${CLICKHOUSE_TMP:?}/01249_data.sql"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_01249"
