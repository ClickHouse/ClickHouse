#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
ROOT="${USER_FILES_PATH}/${PREFIX}"

cleanup()
{
    for suffix in a b c d; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PREFIX}_${suffix}"
    done
    rm -rf "${ROOT}"
}
trap cleanup EXIT

# Prints the field ids present in lower_bounds / upper_bounds, plus the decoded Int32 bounds of
# field id 1 (the `key` column, always Int32 and always serializable here).
report()
{
    local table_dir="$1"
    for manifest in $(find "${table_dir}/metadata" -maxdepth 1 -name '*.avro' \
            -not -name 'snap-*.avro' -type f | sort); do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                'lower_ids=' || toString(arraySort(arrayMap(x -> x.1, tupleElement(data_file, 'lower_bounds')))),
                'upper_ids=' || toString(arraySort(arrayMap(x -> x.1, tupleElement(data_file, 'upper_bounds')))),
                'key_lower=' || toString(arrayMap(x -> reinterpretAsInt32(x.2),
                    arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'lower_bounds')))),
                'key_upper=' || toString(arrayMap(x -> reinterpretAsInt32(x.2),
                    arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'upper_bounds'))))
            FROM file('${manifest}', Avro)
            ORDER BY 1, 2, 3, 4
        "
    done
}

# Case A: an unsupported column type (Array(Int32)) must not suppress the bounds of `key`.
echo '--- A: key Int32 + arr Array(Int32)'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PREFIX}_a (key Int32, arr Array(Int32))
    ENGINE = IcebergLocal('${ROOT}/a/')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${PREFIX}_a SELECT number::Int32, [number, number] FROM numbers(5)"
report "${ROOT}/a"

# Case B: an entirely-NULL Nullable column yields a Null bound, which must not suppress `key` either.
echo '--- B: key Int32 + opt Nullable(Int32) all NULL'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PREFIX}_b (key Int32, opt Nullable(Int32))
    ENGINE = IcebergLocal('${ROOT}/b/')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${PREFIX}_b SELECT number::Int32, NULL FROM numbers(5)"
report "${ROOT}/b"

# Case C: control. All columns serializable, so both field ids keep their bounds exactly as before.
echo '--- C: key Int32 + val String (control, all supported)'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PREFIX}_c (key Int32, val String)
    ENGINE = IcebergLocal('${ROOT}/c/')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${PREFIX}_c SELECT number::Int32, 'val-' || toString(number) FROM numbers(5)"
report "${ROOT}/c"

# Case D: wrapper composition with a non-NULL value. Float64 is unsupported for bounds even though
# the byte dumper handles it, so the filter must gate on the bounds predicate and skip only `f`.
echo '--- D: key Int32 + f Nullable(Float64) with values'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PREFIX}_d (key Int32, f Nullable(Float64))
    ENGINE = IcebergLocal('${ROOT}/d/')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
    "INSERT INTO ${PREFIX}_d SELECT number::Int32, number + 0.5 FROM numbers(5)"
report "${ROOT}/d"
