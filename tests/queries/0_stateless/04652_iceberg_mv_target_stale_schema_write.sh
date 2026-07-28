#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An INSERT that reaches an Iceberg table through a MATERIALIZED VIEW used to write the
# data file against the table's CREATE-time ClickHouse types instead of its live Iceberg
# schema, because `InsertDependenciesBuilder::observePath` captured the metadata snapshot
# without refreshing the external dynamic metadata first. The Iceberg reader maps
# `timestamp` to `DateTime64(6)`, so a `DateTime` value was written as a bare Avro `int`
# and read back as microseconds.
#
# Every arm uses a FRESH target: the defect was first-insert-only per storage instance
# (the first refresh persists), so a shared target would silently self-heal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/test_iceberg_mv_stale_${CLICKHOUSE_DATABASE}"
trap "rm -rf '${BASE_DIR}' 2>/dev/null" EXIT
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}"

CLIENT="${CLICKHOUSE_CLIENT} --session_timezone UTC --allow_insert_into_iceberg=1 --async_insert=0"

# arm_name, column type, write format, inserted literal
run_arm() {
    local arm="$1" col_type="$2" format="$3" value="$4"

    local src="src_${arm}_${CLICKHOUSE_DATABASE}"
    local tgt="tgt_${arm}_${CLICKHOUSE_DATABASE}"
    local mv="mv_${arm}_${CLICKHOUSE_DATABASE}"
    local tgt_path="${BASE_DIR}/${arm}/"

    ${CLIENT} --query "
        CREATE TABLE ${src} (c0 ${col_type}) ENGINE = MergeTree ORDER BY tuple();
        CREATE TABLE ${tgt} (c0 ${col_type}) ENGINE = IcebergLocal('${tgt_path}', '${format}');
        CREATE MATERIALIZED VIEW ${mv} TO ${tgt} AS SELECT c0 FROM ${src};
        INSERT INTO ${src} VALUES (${value});
    "
    echo -n "${arm}: "
    ${CLIENT} --query "SELECT toString(c0) FROM ${tgt}" 2>&1 | head -1

    ${CLIENT} --query "DROP TABLE IF EXISTS ${mv} SYNC; DROP TABLE IF EXISTS ${src} SYNC; DROP TABLE IF EXISTS ${tgt} SYNC"
}

# Corruption carriers.
run_arm datetime          "DateTime"        Avro "'2020-01-02 03:04:05'"
run_arm datetime64_3      "DateTime64(3)"   Avro "'2020-01-02 03:04:05.123'"
# DateTime64(0) and DateTime64(9) used to produce a file that could not be read back at all
# ("Cannot insert Avro decimal with scale N ... scale 6").
run_arm datetime64_0      "DateTime64(0)"   Avro "'2020-01-02 03:04:05'"
run_arm datetime64_9      "DateTime64(9)"   Avro "'2020-01-02 03:04:05.123456789'"
run_arm array_datetime    "Array(DateTime)" Avro "[toDateTime('2020-01-02 03:04:05')]"

# Controls: correct before and after the fix.
run_arm datetime64_6_ctl  "DateTime64(6)"   Avro    "'2020-01-02 03:04:05.123456'"
run_arm parquet_ctl       "DateTime"        Parquet "'2020-01-02 03:04:05'"

# `INSERT INTO <mv>` reaches the target through the same `observePath` line (the view is
# the root, its target is observed as a dependent non-view node).
SRC="src_insert_mv_${CLICKHOUSE_DATABASE}"
TGT="tgt_insert_mv_${CLICKHOUSE_DATABASE}"
MV="mv_insert_mv_${CLICKHOUSE_DATABASE}"
${CLIENT} --query "
    CREATE TABLE ${SRC} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${TGT} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/insert_mv/', 'Avro');
    CREATE MATERIALIZED VIEW ${MV} TO ${TGT} AS SELECT c0 FROM ${SRC};
    INSERT INTO ${MV} VALUES ('2020-01-02 03:04:05');
"
echo -n "insert_into_mv: "
${CLIENT} --query "SELECT toString(c0) FROM ${TGT}" 2>&1 | head -1

# Physical oracle, independent of how the reader interprets the file: the Avro schema in
# the data file header must carry the microsecond logical type, not a bare int.
echo -n "avro_logical_type: "
if grep -aoh 'timestamp-micros' "${BASE_DIR}"/insert_mv/data/*.avro 2>/dev/null | head -1 | grep -q .; then
    echo "timestamp-micros"
else
    echo "MISSING"
fi

${CLIENT} --query "DROP TABLE IF EXISTS ${MV} SYNC; DROP TABLE IF EXISTS ${SRC} SYNC; DROP TABLE IF EXISTS ${TGT} SYNC"
