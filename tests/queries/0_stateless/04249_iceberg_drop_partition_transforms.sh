#!/usr/bin/env bash
# Tags: no-fasttest
#
# Exercises `ALTER TABLE ... DROP PARTITION <expr>` against every Iceberg
# partition transform ClickHouse can write: identity, icebergBucket,
# icebergTruncate, toYearNumSinceEpoch, toMonthNumSinceEpoch, toRelativeDayNum,
# toRelativeHourNum, plus a multi-column case. The user supplies a partition
# value as either a literal matching the stored partition-key type, or as the
# same transform expression applied to a raw source value -- both are accepted,
# mirroring `MergeTree` semantics.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run_scenario() {
    local title=$1
    local schema=$2
    local partition_by=$3
    local insert_values=$4
    local drop_expr=$5
    local select_expr=$6

    local table="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
    local table_path="${USER_FILES_PATH}/${table}/"

    echo "=== ${title} ==="

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} ${schema}
        ENGINE = IcebergLocal('${table_path}', 'Parquet')
        PARTITION BY ${partition_by}
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES ${insert_values}"

    echo "--- before drop ---"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${select_expr} FROM ${table} ORDER BY ALL"

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${table} DROP PARTITION ${drop_expr}"

    echo "--- after drop ---"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${select_expr} FROM ${table} ORDER BY ALL"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "${table_path}"
}

###############################################################################
# 1. identity -- both literal and expression forms
###############################################################################
run_scenario \
    "identity, scalar literal" \
    "(a Int64, b String)" \
    "(identity(a))" \
    "(1, 'x'), (2, 'y'), (3, 'z')" \
    "2" \
    "a, b"

run_scenario \
    "identity, tuple-literal form" \
    "(a Int64, b String)" \
    "(identity(a))" \
    "(1, 'x'), (2, 'y'), (3, 'z')" \
    "tuple(2)" \
    "a, b"

###############################################################################
# 2. icebergBucket -- expression form (user does not need to compute the bucket)
###############################################################################
run_scenario \
    "icebergBucket, expression form" \
    "(id Int64, k String)" \
    "(icebergBucket(4, k))" \
    "(1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')" \
    "tuple(icebergBucket(4, 'banana'))" \
    "id, k"

###############################################################################
# 3. icebergTruncate on String -- truncated prefix as a literal
###############################################################################
run_scenario \
    "icebergTruncate(3, String), literal form" \
    "(id Int64, k String)" \
    "(icebergTruncate(3, k))" \
    "(1, 'apple'), (2, 'apricot'), (3, 'banana'), (4, 'blueberry')" \
    "'app'" \
    "id, k"

run_scenario \
    "icebergTruncate(3, String), expression form" \
    "(id Int64, k String)" \
    "(icebergTruncate(3, k))" \
    "(1, 'apple'), (2, 'apricot'), (3, 'banana'), (4, 'blueberry')" \
    "tuple(icebergTruncate(3, 'apricot'))" \
    "id, k"

###############################################################################
# 4. toYearNumSinceEpoch
###############################################################################
run_scenario \
    "toYearNumSinceEpoch(Date)" \
    "(id Int64, d Date)" \
    "(toYearNumSinceEpoch(d))" \
    "(1, '2024-03-15'), (2, '2024-08-01'), (3, '2025-01-10'), (4, '2025-12-31')" \
    "tuple(toYearNumSinceEpoch(toDate('2024-06-01')))" \
    "id, d"

###############################################################################
# 5. toMonthNumSinceEpoch
###############################################################################
run_scenario \
    "toMonthNumSinceEpoch(Date)" \
    "(id Int64, d Date)" \
    "(toMonthNumSinceEpoch(d))" \
    "(1, '2025-01-05'), (2, '2025-01-29'), (3, '2025-02-10'), (4, '2025-03-15')" \
    "tuple(toMonthNumSinceEpoch(toDate('2025-01-15')))" \
    "id, d"

###############################################################################
# 6. toRelativeDayNum
###############################################################################
run_scenario \
    "toRelativeDayNum(Date)" \
    "(id Int64, d Date)" \
    "(toRelativeDayNum(d))" \
    "(1, '2025-05-01'), (2, '2025-05-01'), (3, '2025-05-02'), (4, '2025-05-03')" \
    "tuple(toRelativeDayNum(toDate('2025-05-01')))" \
    "id, d"

###############################################################################
# 7. toRelativeHourNum
###############################################################################
run_scenario \
    "toRelativeHourNum(DateTime)" \
    "(id Int64, ts DateTime)" \
    "(toRelativeHourNum(ts))" \
    "(1, '2025-05-19 10:15:00'), (2, '2025-05-19 10:45:00'), (3, '2025-05-19 11:00:00'), (4, '2025-05-19 12:00:00')" \
    "tuple(toRelativeHourNum(toDateTime('2025-05-19 10:30:00')))" \
    "id, ts"

###############################################################################
# 8. Multi-column partition: identity + icebergBucket. Drops one partition
#    using the `tuple(<expr>, <expr>)` form and another using the bare
#    tuple-literal form `(a, b)` with a pre-computed bucket index.
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_multi"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
echo "=== multi-column: identity + icebergBucket ==="
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String, payload String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (identity(a), icebergBucket(4, b))
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES
        (1, 'apple', 'p1'),
        (1, 'banana', 'p2'),
        (2, 'apple', 'p3'),
        (2, 'banana', 'p4'),
        (3, 'cherry', 'p5')
"
echo "--- before drop ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b, payload FROM ${TABLE} ORDER BY ALL"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} DROP PARTITION tuple(1, icebergBucket(4, 'apple'))
"
echo "--- after drop tuple(1, icebergBucket(4, 'apple')) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b, payload FROM ${TABLE} ORDER BY ALL"

BUCKET_BANANA=$(${CLICKHOUSE_CLIENT} --query "SELECT icebergBucket(4, 'banana')")
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} DROP PARTITION (2, ${BUCKET_BANANA})
"
echo "--- after drop (2, <bucket of 'banana'>) ---"
${CLICKHOUSE_CLIENT} --query "SELECT a, b, payload FROM ${TABLE} ORDER BY ALL"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

###############################################################################
# 9. Arity mismatch is rejected with a clear error code.
###############################################################################
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_arity"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a, b)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 'x')"

echo "=== arity mismatch is rejected ==="
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1" 2>&1 | grep -o 'INVALID_PARTITION_VALUE' | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
