#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
# `StorageObjectStorageCluster` (used when `parallel_replicas_for_cluster_engines = 1`,
# default) does not delegate `supportsPrewhere` to its underlying configuration,
# so explicit `PREWHERE` against `icebergLocal` is rejected by the analyzer.
#
# Regression test for the schema-changed branch of the Iceberg PARQUET `PREWHERE`
# path. When an Iceberg table has gone through a column RENAME and then a query
# uses `PREWHERE` on the renamed column against PARQUET data files, the source:
#
#   1) reads the parquet file using a `ColumnMapper` that resolves parquet
#      field-ids to the FILE-side (old) clickhouse names (the schema id current
#      when the file was written),
#   2) applies a `schema_transform` `ExpressionTransform` that renames the
#      file-side names into the QUERY-side names of the current schema.
#
# Parquet supports `PREWHERE`, so before the fix the `PREWHERE` / row-level
# filter were pushed INTO the parquet reader together with that old-schema
# mapper. `PREWHERE` references the CURRENT name (`renamed_c0`) which the
# old-schema mapper does not know, so the reader materialized an absent/default
# column and the predicate matched nothing -> the query silently returned 0
# rows (a wrong result, not an error). `WHERE` on the same column was correct
# because it runs as a `FilterTransform` after `schema_transform`.
#
# The fix strips `PREWHERE` / row-level filter on the schema-changed Iceberg
# path (regardless of format) and replays them as fallback `FilterTransform`s
# after `schema_transform` aliases the names, exactly like the ORC path. This
# is the PARQUET data-file counterpart of the ORC-only coverage in
# `04147_iceberg_orc_schema_evolution_row_policy.sh`.
#
# Without the fix (parquet data files):
#   `SELECT count() FROM t PREWHERE renamed_c0 > 10` -> 0 (should be 89)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_parquet_evol"
TEST_USER="${CLICKHOUSE_DATABASE}_user_pq_evol"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy_pq_evol"
TEST_TABLE="t_ice_parquet_evol"

rm -rf "${ICEBERG_PATH}"

# Create an Iceberg table with PARQUET data files under its first schema
# (c0 Int64, c1 String, c2 Int32). One data file, 100 rows.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE} (c0 Int64, c1 String, c2 Int32)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO ${TEST_TABLE}
        SELECT number, toString(number), toInt32(number) FROM numbers(100);
"

# Rename c0 -> renamed_c0. The existing parquet data file still carries the
# column-id under its old name `c0`; the current snapshot has it as
# `renamed_c0`. Reads now go through the schema-changed path.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TEST_TABLE} RENAME COLUMN c0 TO renamed_c0;
"

# 1) PREWHERE on the renamed column, not in the SELECT list
#    (remove_prewhere_column = true). Rows with renamed_c0 in (10..99] -> 89.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TEST_TABLE} PREWHERE renamed_c0 > 10
"

# 2) PREWHERE on the renamed column plus a WHERE on another (unrenamed) column
#    kept in the SELECT list. Rows with renamed_c0 > 10 AND c1 starting with
#    '1' -> {1x} in (10..99]: 12..19 and 100 excluded -> 9.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TEST_TABLE} PREWHERE renamed_c0 > 10 WHERE startsWith(c1, '1')
"

# 3) Read another column with PREWHERE on the renamed column only (the renamed
#    column is dropped after PREWHERE). sum of c1 (as Int) for renamed_c0 in
#    (10..99] = sum(11..99) = 4895.
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(toInt64(c1)) FROM ${TEST_TABLE} PREWHERE renamed_c0 > 10
"

# 4) Control: PREWHERE on a NON-renamed column (c2) must stay correct. Rows
#    with c2 < 50 -> 50.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TEST_TABLE} PREWHERE c2 < 50
"

# 5) Same predicate as (1) but via WHERE — always worked (fallback filter runs
#    after schema_transform). Kept as an oracle: PREWHERE must equal WHERE. 89.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TEST_TABLE} WHERE renamed_c0 > 10
"

# 6) Row policy on the renamed column, combined with PREWHERE on it. The policy
#    keeps renamed_c0 > 5; PREWHERE keeps renamed_c0 > 10; intersection is
#    (10..99] -> 89. Exercises row_level_filter on the schema-changed parquet
#    path (same reader-side name-resolution as PREWHERE).
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'pq_pwd_evol'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${TEST_TABLE} FOR SELECT USING renamed_c0 > 5 TO ${TEST_USER}"

${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=pq_pwd_evol --query "
    SELECT count() FROM ${TEST_TABLE} PREWHERE renamed_c0 > 10
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
