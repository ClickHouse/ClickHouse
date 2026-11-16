#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_cast_overflow"

rm -rf "$TABLE_PATH"

# Create a Delta table whose only column is stored as Delta `byte` (Int8), then drop it so the
# `_delta_log` remains on disk. Re-attaching with a wider declared type (`Int32`) keeps the declared
# type on the ClickHouse side while the write schema stays `Int8`, so an INSERT casts `Int32` -> `Int8`.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;

DROP TABLE IF EXISTS t_dl_cast;
CREATE TABLE t_dl_cast (id Int8) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
DROP TABLE t_dl_cast;
"

# Default (delta_lake_accurate_write_cast = 1): a value that does not fit `Int8` throws instead of
# being silently truncated.
if $CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
CREATE TABLE t_dl_cast (id Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
INSERT INTO t_dl_cast VALUES (1000);
" 2>&1 | grep -q "cannot be safely converted"; then echo "overflow rejected"; else echo "overflow NOT rejected"; fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_dl_cast"

# With delta_lake_accurate_write_cast = 0 the plain cast is used: the value is silently
# truncated, so the INSERT succeeds (the row is written).
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
SET delta_lake_accurate_write_cast = 0;
CREATE TABLE t_dl_cast (id Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
INSERT INTO t_dl_cast VALUES (1000);
SELECT count() FROM t_dl_cast;
DROP TABLE t_dl_cast;
"

rm -rf "$TABLE_PATH"

# With `compatibility` set below 26.9, delta_lake_accurate_write_cast defaults to 0 (the plain,
# non-throwing cast), so the overflowing INSERT succeeds without an explicit per-query override.
COMPAT_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_cast_overflow_compat"
rm -rf "$COMPAT_PATH"
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
CREATE TABLE t_dl_cast_compat (id Int8) ENGINE = DeltaLakeLocal('${COMPAT_PATH}', Parquet);
DROP TABLE t_dl_cast_compat;
"
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
SET compatibility = '25.8';
CREATE TABLE t_dl_cast_compat (id Int32) ENGINE = DeltaLakeLocal('${COMPAT_PATH}', Parquet);
INSERT INTO t_dl_cast_compat VALUES (1000);
SELECT count() FROM t_dl_cast_compat;
DROP TABLE t_dl_cast_compat;
"

rm -rf "$COMPAT_PATH"
