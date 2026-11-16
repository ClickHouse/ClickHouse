#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `no-parallel-replicas`: see comment in `04071_iceberg_orc_prewhere_crash.sh`.
# `StorageObjectStorageCluster` (used when `parallel_replicas_for_cluster_engines = 1`,
# default) does not delegate `supportsPrewhere` to its underlying configuration,
# so explicit `PREWHERE` against `icebergLocal` is rejected by the analyzer.
#
# Regression test for the schema-changed branch of the Iceberg ORC `PREWHERE`
# fallback path. When an Iceberg table has gone through schema evolution
# (e.g. `ALTER TABLE ... RENAME COLUMN`) and then receives a query against
# ORC data files with a row policy and/or `PREWHERE`, the source pipeline:
#
#   1) reads ORC data files using their FILE-side column names (the schema id
#      that was current when the file was written),
#   2) applies a `schema_transform` `ExpressionTransform` that renames /
#      typecasts / fills defaults to convert FILE-side names into the
#      QUERY-side names of the current schema,
#   3) applies the fallback `FilterTransform`s for `row_level_filter` and
#      `PREWHERE` (the format reader couldn't apply them because ORC doesn't
#      support `PREWHERE`).
#
# Filter expressions are built by the planner against QUERY-side column names.
# They must therefore run AFTER `schema_transform`, otherwise they reference
# columns that aren't yet present in the block (the block still has FILE-side
# names) and the query fails with `NOT_FOUND_COLUMN_IN_BLOCK`.
#
# Without the fix:
#   `Code: 10. DB::Exception: Not found column renamed_c0: in block c0 Int64...`
#
# This test deliberately uses ORC files only (created via the `icebergLocal`
# table function) so that every read goes through the strip-and-replay
# fallback. The mixed-format Parquet+ORC variant is covered by
# `04146_iceberg_orc_row_policy_prewhere.sh` for the non-schema-changed case.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_evol"
TEST_USER="${CLICKHOUSE_DATABASE}_user_evol"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy_evol"
TEST_TABLE="t_ice_orc_evol"

rm -rf "${ICEBERG_PATH}"

# Create an Iceberg table at format=Parquet (so the table-level `PREWHERE`
# check passes and we exercise the strip/fallback path) but populate it with
# ORC data files only via the `icebergLocal` table function. After this we
# have one ORC file under the table's first schema.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;

    CREATE TABLE ${TEST_TABLE} (c0 Int64, c1 String)
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'c0 Int64, c1 String')
        SELECT number, toString(number) FROM numbers(100);
"

# Bump the schema by renaming `c0` to `renamed_c0`. Existing data files still
# reference the column-id under its old name `c0` on disk; the current snapshot
# has the same column-id under the new name `renamed_c0`. Reads now go through
# the schema-changed path: format reader emits `c0`, then `schema_transform`
# renames it to `renamed_c0` downstream of the source.
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TEST_TABLE} RENAME COLUMN c0 TO renamed_c0;
"

# Set up a user and a row policy on the renamed column. The policy keeps rows
# where `renamed_c0 > 5`.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'rp_pwd_evol'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${TEST_TABLE} FOR SELECT USING renamed_c0 > 5 TO ${TEST_USER}"

# 1) `PREWHERE` on the renamed column where it is NOT in the SELECT list (so
#    `remove_prewhere_column = true`). Without the fix the row-policy
#    expression `renamed_c0 > 5` cannot evaluate against the ORC file's block
#    because the block still has the file-side name `c0`. With the fix the
#    fallback filter runs after `schema_transform`, so it sees `renamed_c0`.
#    Expected: rows where `renamed_c0` in (10..99]; all are >5 so the policy
#    is satisfied. count = 89.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd_evol --query "
    SELECT count()
    FROM ${TEST_TABLE}
    PREWHERE renamed_c0 > 10
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# 2) `PREWHERE` overlaps the policy boundary (`renamed_c0 > 3`, policy
#    `> 5`). Policy is the tighter filter — count must reflect
#    `renamed_c0 > 5` (intersection), i.e. rows in (5..99]. count = 94.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd_evol --query "
    SELECT count()
    FROM ${TEST_TABLE}
    PREWHERE renamed_c0 > 3
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# 3) Read `c1` with `PREWHERE` on the renamed column only (column gets removed
#    after `PREWHERE`). Tests that `schema_transform` preserves both the
#    `PREWHERE`-input column (so the `FilterTransform` can evaluate it) and
#    the SELECT-list column. Expected: sum of integer values of `c1` for rows
#    where `renamed_c0` in (10..99] = sum(11..99) = 4895.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd_evol --query "
    SELECT sum(toInt64(c1))
    FROM ${TEST_TABLE}
    PREWHERE renamed_c0 > 10
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# 4) Plain `SELECT` without `PREWHERE` — exercises the row policy alone on
#    the renamed column under the schema-changed path. Expected: rows where
#    `renamed_c0 > 5`, i.e. rows in (5..99]. count = 94.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd_evol --query "
    SELECT count()
    FROM ${TEST_TABLE}
    SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
