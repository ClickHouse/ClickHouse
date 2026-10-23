#!/usr/bin/env bash
# Tags: long, no-fasttest

# --dump-schema cases from review: limits, dialect, stored settings, table functions reading a local table, cast types,
# codec settings, a non-Atomic `default`, SQL UDFs in a probed view and an `Overlay` over a materialized view.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# clickhouse-local refuses --dump-schema with a file, pipe or socket on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps them with extra options and replays the dump into a fresh target.
round_trip() {
    local name=$1 ddl=$2 dbs=$3
    shift 3
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" --dump-schema "$dbs" > "$work/$name.sql" 2> "$work/$name.err"
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

# A result limit with overflow mode `break` must fail the dump, not cut the catalog short.
rm -rf "$work/src"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.t1 (x UInt8) ENGINE = Memory;
    CREATE TABLE d.t2 (x UInt8) ENGINE = Memory;
    CREATE TABLE d.t3 (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_LOCAL --path "$work/src" --max_result_rows 1 --result_overflow_mode break --max_block_size 1 --dump-schema > "$work/limit.sql" 2>/dev/null
rc=$?
echo "limit: $([ $rc -ne 0 ] && echo 'dump failed' || echo "dumped $(grep -c '^CREATE TABLE' "$work/limit.sql") tables")"

round_trip dialect "
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt8) ENGINE = Memory;" d --dialect prql

# A stored query with its own SETTINGS needs no gate.
round_trip query_settings "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW d.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM d.src GROUP BY x SETTINGS max_threads = 1;" d

# The table sorts before the table its table function reads at CREATE.
round_trip merge_tree_index "
    CREATE DATABASE d;
    CREATE TABLE d.zzz_src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.a_r ENGINE = Remote('127.0.0.1', mergeTreeIndex('d', 'zzz_src'));" d

round_trip cast_or_default "
    SET allow_suspicious_fixed_string_types = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64, s String DEFAULT toTypeName(accurateCastOrDefault('a', 'FixedString(257)'))) ENGINE = MergeTree ORDER BY x;" d

round_trip codec_setting "
    SET enable_zxc_codec = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';" d

# clickhouse-local shows its own `default` as `Overlay`, which is not reported.
round_trip default_engine "
    CREATE TABLE default.t (x UInt8) ENGINE = MergeTree ORDER BY x;" default
grep -c 'database `default` has engine' "$work/default_engine.err" || true

# The session's `limit` must not cut the dump's own catalog reads short.
rm -rf "$work/src"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.t1 (x UInt8) ENGINE = Memory;
    CREATE TABLE d.t2 (x UInt8) ENGINE = Memory;"
$CLICKHOUSE_LOCAL --path "$work/src" --limit 1 --dump-schema d > "$work/limit_setting.sql" 2>/dev/null
echo "limit_setting: rc=$? tables=$(grep -c '^CREATE TABLE' "$work/limit_setting.sql")"

# The probe must not expand a SQL UDF, whose scalar subquery would run on the source (here for 10 seconds).
rm -rf "$work/src"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    SET allow_materialized_view_with_bad_select = 1;
    CREATE MATERIALIZED VIEW d.mv (y UInt64) ENGINE = MergeTree ORDER BY y AS SELECT later_udf_${CLICKHOUSE_TEST_UNIQUE_NAME}(x) AS y FROM d.src;
    CREATE FUNCTION later_udf_${CLICKHOUSE_TEST_UNIQUE_NAME} AS x -> (SELECT sum(sleepEachRow(1)) FROM numbers(10) SETTINGS max_block_size = 1) + x;"
start=$(date +%s)
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/udf.sql" 2>/dev/null
echo "udf_probe: rc=$? $([ $(( $(date +%s) - start )) -ge 10 ] && echo 'ran the function body' || echo 'did not run the function body')"

# The table sorts before the TimeSeries table its PromQL table function reads at CREATE.
rm -rf "$work/src" "$work/dst"
$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    SET allow_experimental_time_series_table = 1;
    CREATE DATABASE d;
    CREATE TABLE d.zzz_src ENGINE = TimeSeries;
    CREATE TABLE d.a_r ENGINE = Remote('127.0.0.1', prometheusQuery('d', 'zzz_src', 'up', 0));"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/prometheus_query.sql" 2>/dev/null
echo "prometheus_query: dump rc=$?"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/prometheus_query.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "prometheus_query: replay rc=${PIPESTATUS[0]}"

# An `Overlay` exposes the materialized view's UUID-named inner table, which no emitted statement names.
round_trip overlay_mv "
    CREATE DATABASE s;
    CREATE TABLE s.src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE MATERIALIZED VIEW s.mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM s.src;
    CREATE DATABASE o ENGINE = Overlay('s');" s,o
