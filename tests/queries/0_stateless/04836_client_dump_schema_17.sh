#!/usr/bin/env bash

# --dump-schema keeps the type gates of a table-function structure that the stored CREATE computes instead of spelling
# it out: in the target of a local Remote table, which the server stores unfolded and resolves at CREATE, and in the
# SELECT of a materialized view, which replay analyzes. A structure the dump cannot resolve keeps every type gate.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source under `allow_suspicious_low_cardinality_types`, dumps them with extra options,
# and replays the dump into a fresh target where the gate is off, after running `$target_setup` there.
target_setup=""
round_trip() {
    local name=$1 ddl=$2
    shift 2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "SET allow_suspicious_low_cardinality_types = 1; $ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" "$@" --dump-schema d > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    [ -z "$target_setup" ] || $CLICKHOUSE_LOCAL --path "$work/dst" -n -q "$target_setup"
    $CLICKHOUSE_LOCAL --path "$work/dst" --allow_suspicious_low_cardinality_types=0 -n --queries-file "$work/$name.sql" 2>&1 \
        | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

# A local Remote table resolves its table-function target at CREATE, while the server keeps the arguments of `input`
# and `url` there as they were written.
round_trip remote_engine "
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = Remote('127.0.0.1', input(concat('x LowCardinality(', 'UInt64)')));"

round_trip view_inner_engine "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv
        ENGINE = Remote('127.0.0.1', url('http://127.0.0.1:1/', CSV, concat('x LowCardinality(', 'UInt64)')))
        AS SELECT x FROM d.src;"

# Replay analyzes the SELECT of a materialized view. The source analyzes it under the gate, so the dump does not relax
# that check with `allow_materialized_view_with_bad_select`.
round_trip view_select "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory
        AS SELECT s.x AS x FROM d.src AS s INNER JOIN generateRandom(concat('x LowCardinality(', 'UInt64)')) AS f ON s.x = f.x;" \
    --allow_suspicious_low_cardinality_types=1

# The analyzer reads an identifier argument as the constant of the alias it names.
round_trip view_alias "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS WITH 'x LowCardinality(UInt64)' AS s
        SELECT t.x AS x FROM d.src AS t INNER JOIN url('http://127.0.0.1:1/', CSV, s) AS f ON t.x = f.x;" \
    --allow_suspicious_low_cardinality_types=1

# Replay folds `currentDatabase` in its own session, which the dump does not share, so every type gate is kept.
round_trip session_dependent "
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64)
        ENGINE = Remote('127.0.0.1', input(concat('x LowCardinality(', if(currentDatabase() = '', 'String', 'UInt64'), ')')));"

# A named collection can carry the structure, which the stored CREATE does not show, so every type gate is kept.
target_setup="CREATE NAMED COLLECTION nc_structure AS
    url = 'http://127.0.0.1:1/', format = 'CSV', structure = 'x LowCardinality(UInt64)';"
round_trip named_collection "
    $target_setup
    CREATE DATABASE d;
    CREATE TABLE d.t (x UInt64) ENGINE = Remote('127.0.0.1', url(nc_structure));"
