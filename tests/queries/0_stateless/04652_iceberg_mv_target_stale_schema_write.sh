#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An INSERT reaching an Iceberg table through a MATERIALIZED VIEW wrote the data file against
# the CREATE-time ClickHouse types instead of the live Iceberg schema, so a `DateTime` was
# written as a bare Avro `int` and read back as microseconds. Every INSERT was affected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# A server warning would land on stderr, which clickhouse-test turns into a failure, or would
# replace an asserted value in the `2>&1 | head -1` arms below.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="none"
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/test_iceberg_mv_stale_${CLICKHOUSE_DATABASE}"
trap "rm -rf '${BASE_DIR}' 2>/dev/null" EXIT
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}"

CLIENT="${CLICKHOUSE_CLIENT} --session_timezone UTC --allow_insert_into_iceberg=1 --async_insert=0"
DB="${CLICKHOUSE_DATABASE}"

# arm_name|column type|write format|inserted literal. datetime64_0 and datetime64_9 used to
# produce a file that could not be read back at all ("Cannot insert Avro decimal with scale N
# ... scale 6"). The last two arms are controls: correct both before and after the fix.
ARMS="datetime|DateTime|Avro|'2020-01-02 03:04:05'
datetime64_3|DateTime64(3)|Avro|'2020-01-02 03:04:05.123'
datetime64_0|DateTime64(0)|Avro|'2020-01-02 03:04:05'
datetime64_9|DateTime64(9)|Avro|'2020-01-02 03:04:05.123456789'
array_datetime|Array(DateTime)|Avro|[toDateTime('2020-01-02 03:04:05')]
datetime64_6_ctl|DateTime64(6)|Avro|'2020-01-02 03:04:05.123456'
parquet_ctl|DateTime|Parquet|'2020-01-02 03:04:05'"

ARM_SETUP=""
ARM_DROP=""
while IFS='|' read -r arm col_type format value; do
    ARM_SETUP="${ARM_SETUP}
    CREATE TABLE src_${arm}_${DB} (c0 ${col_type}) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tgt_${arm}_${DB} (c0 ${col_type}) ENGINE = IcebergLocal('${BASE_DIR}/${arm}/', '${format}');
    CREATE MATERIALIZED VIEW mv_${arm}_${DB} TO tgt_${arm}_${DB} AS SELECT c0 FROM src_${arm}_${DB};
    INSERT INTO src_${arm}_${DB} VALUES (${value});"
    ARM_DROP="${ARM_DROP}
    DROP TABLE mv_${arm}_${DB} SYNC; DROP TABLE src_${arm}_${DB} SYNC; DROP TABLE tgt_${arm}_${DB} SYNC;"
done <<< "${ARMS}"

# repeat: both rows must read back correctly, since every INSERT was corrupt. shared: two views
# writing to ONE target, which is observed once per view, so the refresh must stay consistent
# across both visits. insert_mv6: control for the insert_mv arm below.
${CLIENT} --query "${ARM_SETUP}

    CREATE TABLE src_repeat_${DB} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tgt_repeat_${DB} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/repeat/', 'Avro');
    CREATE MATERIALIZED VIEW mv_repeat_${DB} TO tgt_repeat_${DB} AS SELECT c0 FROM src_repeat_${DB};
    INSERT INTO src_repeat_${DB} VALUES ('2020-01-02 03:04:05');
    INSERT INTO src_repeat_${DB} VALUES ('2021-02-03 04:05:06');

    CREATE TABLE src_shared_${DB} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tgt_shared_${DB} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/shared/', 'Avro');
    CREATE MATERIALIZED VIEW mv_shared_a_${DB} TO tgt_shared_${DB} AS SELECT c0 FROM src_shared_${DB};
    CREATE MATERIALIZED VIEW mv_shared_b_${DB} TO tgt_shared_${DB} AS SELECT c0 + toIntervalDay(1) AS c0 FROM src_shared_${DB};
    INSERT INTO src_shared_${DB} VALUES ('2020-01-02 03:04:05');

    CREATE TABLE src_insert_mv_${DB} (c0 DateTime) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tgt_insert_mv_${DB} (c0 DateTime) ENGINE = IcebergLocal('${BASE_DIR}/insert_mv/', 'Avro');
    CREATE MATERIALIZED VIEW mv_insert_mv_${DB} TO tgt_insert_mv_${DB} AS SELECT c0 FROM src_insert_mv_${DB};

    CREATE TABLE src_insert_mv6_${DB} (c0 DateTime64(6)) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tgt_insert_mv6_${DB} (c0 DateTime64(6)) ENGINE = IcebergLocal('${BASE_DIR}/insert_mv6/', 'Avro');
    CREATE MATERIALIZED VIEW mv_insert_mv6_${DB} TO tgt_insert_mv6_${DB} AS SELECT c0 FROM src_insert_mv6_${DB};
    INSERT INTO mv_insert_mv6_${DB} VALUES ('2020-01-02 03:04:05.123456');
"

# One read per arm: a batched read would stop at the first arm that throws, hiding both the
# remaining carriers and the two controls that must stay correct.
while IFS='|' read -r arm _; do
    echo -n "${arm}: "
    ${CLIENT} --query "SELECT toString(c0) FROM tgt_${arm}_${DB} FORMAT TSVRaw" 2>&1 | head -1
done <<< "${ARMS}"

echo -n "repeat_inserts: "
${CLIENT} --query "SELECT arrayStringConcat(groupArray(toString(c0)), ' ') FROM (SELECT c0 FROM tgt_repeat_${DB} ORDER BY c0) FORMAT TSVRaw" 2>&1 | head -1
echo -n "shared_target: "
${CLIENT} --query "SELECT arrayStringConcat(groupArray(toString(c0)), ' ') FROM (SELECT c0 FROM tgt_shared_${DB} ORDER BY c0) FORMAT TSVRaw" 2>&1 | head -1

# `INSERT INTO <mv>` raises TYPE_MISMATCH because a view's own declared columns stay `DateTime`
# while the pre-sink converter only retypes widened Enums. That is a separate pre-existing bug
# that also happens on plain MergeTree; pin only that it is fail-closed, never a corrupt row.
echo -n "insert_into_mv_type_mismatch: "
${CLIENT} --query "INSERT INTO mv_insert_mv_${DB} VALUES ('2020-01-02 03:04:05')" 2>&1 | grep -om1 'Code: 53' || echo 'NO_ERROR'

echo -n "insert_into_mv_rows: "
${CLIENT} --query "SELECT count() FROM tgt_insert_mv_${DB}" 2>&1 | head -1
# `count()` on Iceberg is answered from metadata, so also count data files on disk: an orphan
# file left behind by a partially built sink would not move the row count.
echo -n "insert_into_mv_data_files: "
find "${BASE_DIR}/insert_mv/data" -name '*.avro' 2>/dev/null | wc -l

echo -n "insert_into_mv_no_widening_ctl: "
${CLIENT} --query "SELECT toString(c0) FROM tgt_insert_mv6_${DB}" 2>&1 | head -1

# Physical oracle, independent of how the reader interprets the file: the Avro schema in the
# first arm's data file must carry the microsecond logical type, not a bare int. It reads the
# files directly, so it has to run before the tables are dropped.
echo -n "avro_logical_type: "
if grep -aoh 'timestamp-micros' "${BASE_DIR}"/datetime/data/*.avro 2>/dev/null | head -1 | grep -q .; then
    echo "timestamp-micros"
else
    echo "MISSING"
fi

${CLIENT} --query "${ARM_DROP}
    DROP TABLE mv_repeat_${DB} SYNC; DROP TABLE src_repeat_${DB} SYNC; DROP TABLE tgt_repeat_${DB} SYNC;
    DROP TABLE mv_shared_a_${DB} SYNC; DROP TABLE mv_shared_b_${DB} SYNC;
    DROP TABLE src_shared_${DB} SYNC; DROP TABLE tgt_shared_${DB} SYNC;
    DROP TABLE mv_insert_mv_${DB} SYNC; DROP TABLE src_insert_mv_${DB} SYNC; DROP TABLE tgt_insert_mv_${DB} SYNC;
    DROP TABLE mv_insert_mv6_${DB} SYNC; DROP TABLE src_insert_mv6_${DB} SYNC; DROP TABLE tgt_insert_mv6_${DB} SYNC;
"
