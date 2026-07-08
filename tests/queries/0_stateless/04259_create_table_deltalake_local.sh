#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH_UNPART="${CLICKHOUSE_USER_FILES_UNIQUE}_unpart"
TABLE_PATH_PART="${CLICKHOUSE_USER_FILES_UNIQUE}_part"
TABLE_PATH_NOKERNEL="${CLICKHOUSE_USER_FILES_UNIQUE}_nokernel"

rm -rf "$TABLE_PATH_UNPART" "$TABLE_PATH_PART" "$TABLE_PATH_NOKERNEL"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

DROP TABLE IF EXISTS t_dl_unpart;
CREATE TABLE t_dl_unpart (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH_UNPART}', Parquet);

INSERT INTO t_dl_unpart SELECT number, toString(number) FROM numbers(5);

SELECT id, name FROM t_dl_unpart ORDER BY id;
SELECT count() FROM t_dl_unpart;

-- IF NOT EXISTS path on an already-created table is a no-op.
CREATE TABLE IF NOT EXISTS t_dl_unpart (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH_UNPART}', Parquet);
SELECT count() FROM t_dl_unpart;

DROP TABLE t_dl_unpart;
"

# PARTITION BY is not yet supported for Delta Lake CREATE TABLE: the delta-kernel-rs
# FFI exposes neither `with_data_layout` (create) nor `partitioned_write_context`
# (insert), so the DeltaLake engine does not enable `supports_sort_order` and
# StorageFactory rejects PARTITION BY / PRIMARY KEY / ORDER BY / SAMPLE BY. Assert the
# rejection instead of silently creating an unpartitioned table. Prints 1 (the error
# was raised); the CREATE fails in StorageFactory before any `_delta_log` is written.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_part (id Int32, name String, country String)
    ENGINE = DeltaLakeLocal('${TABLE_PATH_PART}', Parquet)
    PARTITION BY country;
" 2>&1 | grep -c "support PARTITION_BY"

# With allow_experimental_delta_kernel_rs = 0 there is no Delta Lake writer, so a fresh CREATE
# (a location with no `_delta_log`) must fail rather than silently creating a ClickHouse table over
# a non-Delta location. Prints 1 (the error names the required setting).
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 0;
CREATE TABLE t_dl_nokernel (id Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH_NOKERNEL}', Parquet);
" 2>&1 | grep -c "requires allow_experimental_delta_kernel_rs"

rm -rf "$TABLE_PATH_UNPART" "$TABLE_PATH_PART" "$TABLE_PATH_NOKERNEL"
