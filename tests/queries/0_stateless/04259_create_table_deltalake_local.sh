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

# PARTITION BY is not yet supported for Delta Lake CREATE TABLE (the kernel FFI exposes neither
# `with_data_layout` nor `partitioned_write_context`), so the engine does not enable
# `supports_sort_order` and StorageFactory rejects PARTITION BY / PRIMARY KEY / ORDER BY / SAMPLE BY.
# Use `grep -q` (existence, not line count): the error is echoed on more than one line.
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_part (id Int32, name String, country String)
    ENGINE = DeltaLakeLocal('${TABLE_PATH_PART}', Parquet)
    PARTITION BY country;
" 2>&1 | grep -q "support PARTITION_BY"; then echo "partition by rejected"; else echo "partition by NOT rejected"; fi

# With allow_experimental_delta_kernel_rs = 0 there is no Delta Lake writer, so a fresh CREATE
# (a location with no `_delta_log`) must fail rather than silently creating a ClickHouse table over
# a non-Delta location.
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 0;
CREATE TABLE t_dl_nokernel (id Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH_NOKERNEL}', Parquet);
" 2>&1 | grep -q "requires allow_experimental_delta_kernel_rs"; then echo "fresh create without kernel rejected"; else echo "fresh create without kernel NOT rejected"; fi

rm -rf "$TABLE_PATH_UNPART" "$TABLE_PATH_PART" "$TABLE_PATH_NOKERNEL"
