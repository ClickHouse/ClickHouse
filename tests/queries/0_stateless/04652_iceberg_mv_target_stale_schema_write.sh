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
# The corruption repeats on every INSERT, not only the first one: nothing on the INSERT path
# refreshed the target, so every new data file was written against the same stale types.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Keep server logs out of this test's own output: shell_config.sh passes
# `--send_logs_level=warning`, so a warning would land on stderr (which clickhouse-test turns
# into a failure) or replace an asserted value in the `2>&1 | head -1` arms below.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="none"
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
    ${CLIENT} --query "SELECT toString(c0) FROM ${tgt} FORMAT TSVRaw" 2>&1 | head -1

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

# Repeated INSERTs: every one used to be corrupt, so both rows must come back correct.
SRC="src_repeat_${CLICKHOUSE_DATABASE}"
TGT="tgt_repeat_${CLICKHOUSE_DATABASE}"
MV="mv_repeat_${CLICKHOUSE_DATABASE}"
${CLIENT} --query "
    CREATE TABLE ${SRC} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${TGT} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/repeat/', 'Avro');
    CREATE MATERIALIZED VIEW ${MV} TO ${TGT} AS SELECT c0 FROM ${SRC};
    INSERT INTO ${SRC} VALUES ('2020-01-02 03:04:05');
    INSERT INTO ${SRC} VALUES ('2021-02-03 04:05:06');
"
echo -n "repeat_inserts: "
${CLIENT} --query "SELECT arrayStringConcat(groupArray(toString(c0)), ' ') FROM (SELECT c0 FROM ${TGT} ORDER BY c0) FORMAT TSVRaw" 2>&1 | head -1
${CLIENT} --query "DROP TABLE IF EXISTS ${MV} SYNC; DROP TABLE IF EXISTS ${SRC} SYNC; DROP TABLE IF EXISTS ${TGT} SYNC"

# Two materialized views on one source, both writing to the SAME Iceberg target. The target is
# observed once per view because dependencies are walked per path, so the refresh has to stay
# consistent across both visits: both rows must read back correctly.
SRC="src_shared_${CLICKHOUSE_DATABASE}"
TGT="tgt_shared_${CLICKHOUSE_DATABASE}"
${CLIENT} --query "
    CREATE TABLE ${SRC} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${TGT} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/shared/', 'Avro');
    CREATE MATERIALIZED VIEW mv_shared_a_${CLICKHOUSE_DATABASE} TO ${TGT} AS SELECT c0 FROM ${SRC};
    CREATE MATERIALIZED VIEW mv_shared_b_${CLICKHOUSE_DATABASE} TO ${TGT} AS SELECT c0 + toIntervalDay(1) AS c0 FROM ${SRC};
    INSERT INTO ${SRC} VALUES ('2020-01-02 03:04:05');
"
echo -n "shared_target: "
${CLIENT} --query "SELECT arrayStringConcat(groupArray(toString(c0)), ' ') FROM (SELECT c0 FROM ${TGT} ORDER BY c0) FORMAT TSVRaw" 2>&1 | head -1
${CLIENT} --query "
    DROP TABLE IF EXISTS mv_shared_a_${CLICKHOUSE_DATABASE} SYNC;
    DROP TABLE IF EXISTS mv_shared_b_${CLICKHOUSE_DATABASE} SYNC;
    DROP TABLE IF EXISTS ${SRC} SYNC; DROP TABLE IF EXISTS ${TGT} SYNC"

# `INSERT INTO <mv>` (into the view, not into its source) reaches the target through the
# same `observePath` line, so the target header is now the Iceberg-derived DateTime64(6).
# A view's own declared columns are user-declared and stay `DateTime`, and the pre-sink
# converter only retypes widened Enums, so this shape raises TYPE_MISMATCH rather than
# writing anything. That limitation is a separate pre-existing bug: it also happens on plain
# MergeTree with no data lake involved. Pin only that the failure is fail-closed - an error and
# an empty target, never a corrupt row. It holds by construction: the throw comes from
# `createPreSink`'s `inner_metadata->check`, before any sink is built, so no file can be written.
SRC="src_insert_mv_${CLICKHOUSE_DATABASE}"
TGT="tgt_insert_mv_${CLICKHOUSE_DATABASE}"
MV="mv_insert_mv_${CLICKHOUSE_DATABASE}"
${CLIENT} --query "
    CREATE TABLE ${SRC} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${TGT} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/insert_mv/', 'Avro');
    CREATE MATERIALIZED VIEW ${MV} TO ${TGT} AS SELECT c0 FROM ${SRC};
"
echo -n "insert_into_mv_type_mismatch: "
${CLIENT} --query "INSERT INTO ${MV} VALUES ('2020-01-02 03:04:05')" 2>&1 | grep -om1 'Code: 53' || echo 'NO_ERROR'
echo -n "insert_into_mv_rows: "
${CLIENT} --query "SELECT count() FROM ${TGT}" 2>&1 | head -1
# `count()` on Iceberg is answered from metadata, so also count data files on disk: an orphan
# file left behind by a partially built sink would not move the row count.
echo -n "insert_into_mv_data_files: "
find "${BASE_DIR}/insert_mv/data" -name '*.avro' 2>/dev/null | wc -l
${CLIENT} --query "DROP TABLE IF EXISTS ${MV} SYNC; DROP TABLE IF EXISTS ${SRC} SYNC; DROP TABLE IF EXISTS ${TGT} SYNC"

# Control for the arm above: without widening, `INSERT INTO <mv>` keeps working.
SRC="src_insert_mv6_${CLICKHOUSE_DATABASE}"
TGT="tgt_insert_mv6_${CLICKHOUSE_DATABASE}"
MV="mv_insert_mv6_${CLICKHOUSE_DATABASE}"
${CLIENT} --query "
    CREATE TABLE ${SRC} (c0 DateTime64(6)) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE ${TGT} (c0 DateTime64(6)) ENGINE = IcebergLocal('${BASE_DIR}/insert_mv6/', 'Avro');
    CREATE MATERIALIZED VIEW ${MV} TO ${TGT} AS SELECT c0 FROM ${SRC};
    INSERT INTO ${MV} VALUES ('2020-01-02 03:04:05.123456');
"
echo -n "insert_into_mv_no_widening_ctl: "
${CLIENT} --query "SELECT toString(c0) FROM ${TGT}" 2>&1 | head -1
${CLIENT} --query "DROP TABLE IF EXISTS ${MV} SYNC; DROP TABLE IF EXISTS ${SRC} SYNC; DROP TABLE IF EXISTS ${TGT} SYNC"

# Physical oracle, independent of how the reader interprets the file: the Avro schema in
# the first arm's data file must carry the microsecond logical type, not a bare int.
echo -n "avro_logical_type: "
if grep -aoh 'timestamp-micros' "${BASE_DIR}"/datetime/data/*.avro 2>/dev/null | head -1 | grep -q .; then
    echo "timestamp-micros"
else
    echo "MISSING"
fi
