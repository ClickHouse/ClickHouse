#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
# `StorageObjectStorageCluster` (used when `parallel_replicas_for_cluster_engines = 1`,
# default) does not delegate `supportsPrewhere` to its underlying configuration,
# so explicit `PREWHERE` against `icebergLocal` is rejected by the analyzer.
#
# Regression test for transform-ordering correctness in the Iceberg ORC PREWHERE
# fallback path. When an Iceberg table is configured with format `Parquet`
# (PREWHERE supported) but contains ORC data files (PREWHERE not supported), we
# strip both `prewhere_info` and `row_level_filter` from `FormatFilterInfo` and
# re-apply them as `FilterTransform`s after the format reader.
#
# The order of these two transforms must be: row-level filter first, PREWHERE
# second. This mirrors the canonical filter pipeline used everywhere else
# (`SourceStepWithFilter::applyPrewhereActions`,
# `MergeTreeSelectProcessor::getPrewhereActions`, `Parquet::Reader`).
#
# A row policy that references the same column as PREWHERE (and where that
# column is not in the SELECT list) used to throw or filter incorrectly: PREWHERE
# ran first and removed the column from the block, so the row-policy expression
# could not be evaluated.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_rp_pw"
TEST_USER="${CLICKHOUSE_DATABASE}_user"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy"
TEST_TABLE="t_ice_orc_rp_pw"

rm -rf "${ICEBERG_PATH}"

# Create an Iceberg table with format=Parquet (so the table-level PREWHERE check
# passes) but write a mix of ORC and Parquet data files into it.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE} (c0 Int64, c1 String)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
    INSERT INTO ${TEST_TABLE} SELECT number, toString(number) FROM numbers(100);

    -- Add ORC data files via table function so the table contains mixed formats.
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'c0 Int64, c1 String')
        SELECT number + 100, toString(number + 100) FROM numbers(50);
"

# Set up a user and a row policy on c0. The policy keeps rows where c0 > 5.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'rp_pwd'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${TEST_TABLE} FOR SELECT USING c0 > 5 TO ${TEST_USER}"

# 1) Query with PREWHERE on c0, where c0 is NOT in the SELECT list. This is the
#    case that triggers `remove_prewhere_column = true`. With the bug (PREWHERE
#    first), the row policy could not evaluate `c0 > 5` because the column was
#    already gone from the block. With the fix (row-level filter first, PREWHERE
#    second), both filters apply correctly. Expected: rows where c0 in (10..149]
#    where the policy `c0 > 5` is automatically satisfied; total = 139.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT count()
    FROM ${TEST_TABLE}
    PREWHERE c0 > 10
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# 2) Same but with PREWHERE that overlaps the policy boundary (c0 > 3 < 5). The
#    policy is the tighter filter — count must reflect c0 > 5 (intersection),
#    not c0 > 3.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT count()
    FROM ${TEST_TABLE}
    PREWHERE c0 > 3
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# 3) Sanity: read c1 with PREWHERE on c0 (c0 in PREWHERE only; the column is
#    removed after PREWHERE). Row policy c0 > 5 still applies. Output is sum of
#    integer values of c1 for rows where c0 in (10..149]: sum(11..149) = 11120.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT sum(toInt64(c1))
    FROM ${TEST_TABLE}
    PREWHERE c0 > 10
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
