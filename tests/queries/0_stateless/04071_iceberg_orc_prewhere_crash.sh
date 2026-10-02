#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Reproduces https://github.com/ClickHouse/ClickHouse/issues/96829
#
# `no-parallel-replicas` is strictly necessary here: when
# `parallel_replicas_for_cluster_engines = 1` (default) and
# `cluster_for_parallel_replicas` is set, `TableFunctionObjectStorage` wraps the
# `icebergLocal` storage in `StorageObjectStorageCluster`, which currently does
# not override `supportsPrewhere` and therefore inherits the default `false`
# from `IStorage`. The analyzer then throws `ILLEGAL_PREWHERE` for any explicit
# `PREWHERE` on the resulting cluster storage. This is a separate engine bug
# (cluster wrapper missing `supportsPrewhere` delegation to the underlying
# configuration) that should be fixed in a follow-up.
# When an Iceberg table has ORC data files but is read with format='Parquet'
# (which enables PREWHERE), the server crashes with:
#   Logical error: 'PREWHERE passed to format that doesn't support it'
# because PREWHERE support is determined at the table level based on configuration
# format (Parquet), but individual files may be in ORC which doesn't support PREWHERE.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_prewhere"

# Cleanup
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    -- Create Iceberg table with ORC format and insert data
    CREATE TABLE t_ice_orc_pw (c0 Int64, c1 String)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'ORC');
    INSERT INTO t_ice_orc_pw SELECT number, toString(number) FROM numbers(100);

    -- Also insert a Parquet data file to have mixed formats
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'Parquet', 'c0 Int64, c1 String')
        SELECT number + 100, toString(number + 100) FROM numbers(50);
"

# Read with Parquet config (enables PREWHERE) — this used to crash on ORC files.
# Use explicit PREWHERE (not WHERE + optimizer) because the query plan optimizer
# cannot push PREWHERE for ObjectStorage tables (getColumnSizes() returns empty).
# Explicit PREWHERE on mixed-format Iceberg tables requires the new query analyzer
# because the old analyzer evaluates PREWHERE through a different code path that
# doesn't go through FormatFilterInfo (our per-file format check). Force enable_analyzer=1
# to ensure PREWHERE is handled by the new pipeline even in old-analyzer CI configs.
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM icebergLocal('${ICEBERG_PATH}', 'Parquet', 'c0 Int64, c1 String')
    PREWHERE c0 > 50
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# Also test pure ORC read through Parquet config
ICEBERG_PATH_ORC="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_only_prewhere"
rm -rf "${ICEBERG_PATH_ORC}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE t_ice_orc_only (c0 Int64, c1 String)
        ENGINE = IcebergLocal('${ICEBERG_PATH_ORC}', 'ORC');
    INSERT INTO t_ice_orc_only SELECT number, toString(number) FROM numbers(100);
"

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM icebergLocal('${ICEBERG_PATH_ORC}', 'Parquet', 'c0 Int64, c1 String')
    PREWHERE c0 > 50
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_ice_orc_pw; DROP TABLE IF EXISTS t_ice_orc_only"
rm -rf "${ICEBERG_PATH}" "${ICEBERG_PATH_ORC}"
