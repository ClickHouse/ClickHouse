#!/usr/bin/env bash
# Nanosecond timestamps (Iceberg v3 `timestamp_ns` / `timestamptz_ns`) in tables written by Java Iceberg
# (`data_iceberg/timestamp_ns`, see `generate.py` there): values keep every nanosecond, and partition
# pruning, min/max pruning and Parquet filter pushdown never change a result. Also covers ClickHouse
# INSERTs into such tables and the documented mapping of `DateTime64(P)` to microsecond `timestamp`.
#
# Tags: no-fasttest, long
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - long: over a hundred table reads, a few seconds each under sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading format version 3 metadata logs a spurious "violates Iceberg specification" warning,
# https://github.com/ClickHouse/ClickHouse/issues/125333
CH="${CLICKHOUSE_CLIENT} --send_logs_level=error --session_timezone=UTC --allow_insert_into_iceberg=1"
NO_PRUNING="use_iceberg_partition_pruning = 0, use_iceberg_manifest_list_partition_pruning = 0, input_format_parquet_filter_push_down = 0, optimize_min_max_from_files = 0, optimize_count_from_files = 0"

# The manifests hold absolute paths of the machine that generated them, so this also checks that
# a table moved to another location stays readable.
DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
cp -R "${CUR_DIR}/data_iceberg/timestamp_ns/." "${DIR}/"

$CH --query "SELECT id, ts, tstz FROM icebergLocal('${DIR}/unpartitioned') ORDER BY id"
$CH --query "DESCRIBE icebergLocal('${DIR}/unpartitioned')"

PREDICATES=(
    "ts = '2024-01-01 00:00:00.123456789'"
    "ts = '2024-01-01 00:00:00.123456788'"
    "ts >= '2024-01-01' AND ts < '2024-01-02'"
    "ts = '2024-01-02 00:00:00'"
    "ts BETWEEN '2024-01-01 23:59:59.999999999' AND '2024-01-02 00:00:00.000000001'"
    "ts = '1969-12-31 23:59:59.999999999'"
    "ts = '1969-12-31 23:59:59.999998500'"
    "ts < '1900-01-01'"
    "ts > '2262-01-01'"
    "ts IS NULL"
    "tstz = toDateTime64('2024-01-01 05:30:00.123456789', 9, 'Asia/Kolkata')"
)

matching_ids()
{
    $CH --query "SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${DIR}/$1') WHERE $2 ORDER BY id) $3"
}

echo "predicates on the unpartitioned table"
for predicate in "${PREDICATES[@]}"; do
    echo "${predicate}: $(matching_ids unpartitioned "${predicate}")"
done

echo "pruning never changes a result"
# One client per table and mode: starting a client per predicate is slow under sanitizers.
matching_ids_for_all()
{
    local table=$1 settings=$2 i=0
    shift 2
    for predicate in "$@"; do
        i=$((i + 1))
        echo "SELECT ${i}, groupArray(id) FROM (SELECT id FROM icebergLocal('${DIR}/${table}') WHERE ${predicate} ORDER BY id) ${settings};"
    done | $CH --multiquery
}
for table in unpartitioned identity day hour bucket; do
    predicates=()
    for predicate in "${PREDICATES[@]}"; do
        # Bucket pruning for pre-1970 values is not spec-conformant yet: `icebergBucket` truncates
        # nanoseconds to microseconds toward zero instead of flooring (bucket 12 instead of 8 for -1 ns),
        # https://github.com/ClickHouse/ClickHouse/issues/125330
        if [ "${table}" = bucket ] && [[ "${predicate}" == *"1969-12-31"* ]]; then
            continue
        fi
        predicates+=("${predicate}")
    done
    diff <(matching_ids_for_all "${table}" "" "${predicates[@]}") \
         <(matching_ids_for_all "${table}" "SETTINGS ${NO_PRUNING}" "${predicates[@]}") | sed "s/^/${table}: /"
    stats="SELECT count(), min(ts), max(ts), min(tstz), max(tstz) FROM icebergLocal('${DIR}/${table}')"
    [ "$($CH --query "${stats}")" = "$($CH --query "${stats} SETTINGS ${NO_PRUNING}")" ] || echo "${table}: statistics differ"
done

echo "bucket transform on nanoseconds hashes the microsecond value"
$CH --query "
    SELECT icebergBucket(2147483647, toDateTime64('2017-11-16 22:31:08.000001001', 9, 'UTC')),
           icebergBucket(16, toDateTime64('2017-11-16 22:31:08.000001001', 9, 'UTC'))
         = icebergBucket(16, toDateTime64('2017-11-16 22:31:08.000001', 6, 'UTC'))"

echo "ClickHouse INSERT into nanosecond tables"
for table in unpartitioned bucket identity; do
    cp -R "${DIR}/${table}" "${DIR}/insert_${table}"
    $CH --query "CREATE TABLE ${table} ENGINE = IcebergLocal('${DIR}/insert_${table}/')"
    $CH --query "INSERT INTO ${table} (id, ts, tstz) VALUES (11, '2025-01-01 00:00:00.123456789', '2025-01-01 00:00:00.123456789')" 2>&1 \
        | grep -o -m1 'Unsupported type for iceberg DateTime64(9)'
    for settings in "" "SETTINGS ${NO_PRUNING}"; do
        $CH --query "SELECT '${table}', groupArray(id), max(ts) FROM ${table} WHERE ts > '2024-06-01' AND ts < '2262-01-01' ${settings}"
    done
    $CH --query "DROP TABLE ${table}"
done

# The declared precision is replaced: the table itself becomes DateTime64(6), digits below a microsecond are
# dropped on INSERT, and bounds are written in microseconds.
echo "DateTime64 columns created by ClickHouse are stored as microsecond timestamp"
for precision in 9 3; do
    TABLE="t_${precision}"
    $CH --query "
        CREATE TABLE ${TABLE} (id Int32, ts DateTime64(${precision})) ENGINE = IcebergLocal('${DIR}/${TABLE}/');
        INSERT INTO ${TABLE} VALUES (1, '2024-01-01 00:00:00.123456789');
        INSERT INTO ${TABLE} VALUES (2, '2024-06-01 00:00:00.987654321');"
    $CH --query "SELECT toTypeName(ts) FROM ${TABLE} LIMIT 1"
    $CH --query "SELECT toTypeName(ts) FROM icebergLocal('${DIR}/${TABLE}/') LIMIT 1"
    for settings in "" "SETTINGS ${NO_PRUNING}"; do
        $CH --query "SELECT groupArray(id), min(ts), max(ts) FROM ${TABLE} WHERE ts < '2024-03-01' ${settings}"
    done
    $CH --query "DROP TABLE ${TABLE}"
done

rm -rf "${DIR}"
