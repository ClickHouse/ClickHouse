#!/usr/bin/env bash
# Tags: no-fasttest
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/85955
# Reading an Iceberg table with Map(Tuple(Int), Tuple(Int)) threw
# an exception (std::out_of_range in unordered_map::at) during Parquet column name remapping.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_map_tuple"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_map_tuple (c0 Map(Tuple(Int32), Tuple(Int32))) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_map_tuple SELECT c0 FROM generateRandom('c0 Map(Tuple(Int32), Tuple(Int32))', 42, 1, 1) LIMIT 1;
"

# Read using the icebergLocal table function so that the SETTINGS clause is honored.
# A persistent CREATE TABLE captures format settings from the global context at
# creation time and ignores per-query SETTINGS, so we must use the table function
# to pin the reader path under flaky-check setting randomization.

# Exercise the Arrow reader path (v3=0) where the original `std::out_of_range` was thrown.
# After the fix, this path still fails with a separate pre-existing `TYPE_MISMATCH`
# (the Arrow reader has its own limitation with `Map(Tuple, Tuple)` in Iceberg), but it no
# longer triggers `unordered_map::at: key not found`. Count the occurrences of that text:
# on master this prints 1 (bug present), on this PR it prints 0 (bug fixed).
#
# Use `SELECT c0 FORMAT Null` rather than `SELECT count()` because trivial-count is
# answered from Iceberg metadata without invoking `ParquetBlockInputFormat`, so it
# would not exercise the Arrow column reader regardless of the reader setting.
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM icebergLocal('${ICEBERG_TABLE_PATH}') FORMAT Null SETTINGS input_format_parquet_use_native_reader_v3 = 0" 2>&1 | grep -c 'unordered_map::at'

# The native reader v3 path reads the table correctly.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${ICEBERG_TABLE_PATH}') SETTINGS input_format_parquet_use_native_reader_v3 = 1"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_map_tuple"
rm -rf "${ICEBERG_TABLE_PATH}"
