#!/usr/bin/env bash

# --dump-schema emits `enable_streaming_queries` for a materialized view whose SELECT reads `STREAM`, which CREATE
# analyzes. The dump is replayed into a fresh target with the gate at default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

$CLICKHOUSE_LOCAL --path "$work/src" -n -q "
    SET enable_streaming_queries = 1;
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT x FROM d.src STREAM;"
$CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d > "$work/dump.sql" 2>/dev/null
echo "dump rc=$?"
grep '^SET ' "$work/dump.sql"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
echo "replay rc=${PIPESTATUS[0]}"
source_definition=$($CLICKHOUSE_LOCAL --path "$work/src" -q "SHOW CREATE TABLE d.mv")
replayed_definition=$($CLICKHOUSE_LOCAL --path "$work/dst" -q "SHOW CREATE TABLE d.mv")
echo "same definition: $([ -n "$source_definition" ] && [ "$source_definition" = "$replayed_definition" ] && echo 1 || echo 0)"
