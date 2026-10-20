#!/usr/bin/env bash
# Tags: long, no-fasttest

# --dump-schema cases from review: result limits, dialect, stored query settings, table functions that read a
# local table, accurateCastOrDefault types, codec settings and a `default` database with another engine.

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

round_trip default_engine "
    DROP DATABASE default;
    CREATE DATABASE default ENGINE = Memory;
    CREATE TABLE default.t (x UInt8) ENGINE = Memory;" default
grep -c 'database `default` has engine Memory' "$work/default_engine.err"
