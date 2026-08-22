#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: Depends on Parquet, ORC and Avro.
#
# Object-storage counterpart of `04891_file_row_policy_default_columns`: filters that the
# format reader cannot evaluate are stripped and re-applied after `AddingDefaultsTransform`,
# and their inputs are added back to the reader header by `appendDeferredFilterInputs`.
#
# A row policy on a tuple element (`t.x`) over an Iceberg table configured with `Parquet`
# (PREWHERE supported, so the policy is pushed into `FormatFilterInfo`) but holding ORC data
# files (PREWHERE not supported, so the policy is stripped) is the smallest query that reaches
# that helper. The subcolumn must be requested from the reader as its parent `t` and rebuilt
# afterwards - asking an ORC file for a column literally named `t.x` yields wrong values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_orc_rp_tuple"
TEST_USER="${CLICKHOUSE_DATABASE}_user"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy"
TEST_TABLE="t_ice_orc_rp_tuple"

rm -rf "${ICEBERG_PATH}"

# Format `Parquet` so the table-level PREWHERE check passes, with a mix of Parquet and ORC
# data files. `StorageObjectStorage` caches `supportsPrewhere()` at CREATE time from this
# session setting, so it has to be set here and not only on the SELECTs.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;

    CREATE TABLE ${TEST_TABLE} (c0 Int64, t Tuple(x Int64, y String))
        ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
    INSERT INTO ${TEST_TABLE} SELECT number, (number, toString(number)) FROM numbers(20);

    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'c0 Int64, t Tuple(x Int64, y String)')
        SELECT number + 100, (number + 100, toString(number + 100)) FROM numbers(10);
"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'rp_pwd'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${TEST_TABLE} FOR SELECT USING t.x > 5 TO ${TEST_USER}"

# The policy keeps `t.x > 5`: c0 in 6..19 from the Parquet files and 100..109 from the ORC ones.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT count(), min(c0), max(c0), sum(c0)
    FROM ${CLICKHOUSE_DATABASE}.${TEST_TABLE}
    SETTINGS input_format_parquet_use_native_reader_v3 = 1
"

# Only the ORC files, where the policy has to be applied after the format reader.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT c0, t.x, t.y
    FROM ${CLICKHOUSE_DATABASE}.${TEST_TABLE}
    WHERE c0 >= 100
    ORDER BY c0
    LIMIT 3
    SETTINGS input_format_parquet_use_native_reader_v3 = 1
"

# The policy column is not in the SELECT list, so it exists in the block only as a filter input.
${CLICKHOUSE_CLIENT} --user="${TEST_USER}" --password=rp_pwd --query "
    SELECT sum(toInt64(t.y))
    FROM ${CLICKHOUSE_DATABASE}.${TEST_TABLE}
    SETTINGS input_format_parquet_use_native_reader_v3 = 1
"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS ${TEST_POLICY} ON ${TEST_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}"
rm -rf "${ICEBERG_PATH}"
