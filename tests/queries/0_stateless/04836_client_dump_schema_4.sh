#!/usr/bin/env bash
# Tags: no-darwin, zookeeper, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_replicated_heavy_create"
ZK_PATH="/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dump_schema/replicated_heavy_create"
REPLAY_ZK_PATH="${ZK_PATH}_replay"
DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_populate.sql"
REPLAY_FILE="${DUMP_FILE}.replay"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_populate.err"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC" >/dev/null 2>&1 || true
    rm -f "$DUMP_FILE" "$REPLAY_FILE" "$ERR_FILE"
}
trap cleanup EXIT

echo '--- POPULATE is not a stored replay-time property ---'
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE ${DB}.src (n UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.src SELECT number FROM numbers(3)"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 -q \
    "CREATE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM ${DB}.src"

stored_populate=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND positionCaseInsensitive(create_table_query, 'POPULATE')")
$CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$DUMP_FILE" 2>"$ERR_FILE"
dump_populate=$(grep -ci 'POPULATE' "$DUMP_FILE" || true)
heavy_gate=$(grep -c '^SET database_replicated_allow_heavy_create' "$DUMP_FILE" || true)
echo "stored CREATE keeps POPULATE: $stored_populate"
echo "dump keeps POPULATE: $dump_populate"
echo "heavy-create gate emitted: $heavy_gate"

# Replay against a fresh Keeper path so replica cleanup timing cannot affect the result.
sed "s|${ZK_PATH}|${REPLAY_ZK_PATH}|g" "$DUMP_FILE" > "$REPLAY_FILE"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB} SYNC"
$CLICKHOUSE_CLIENT --database_replicated_allow_heavy_create=0 --distributed_ddl_output_mode=none \
    --multiquery --queries-file "$REPLAY_FILE" > /dev/null 2>"$ERR_FILE"
echo 'OK: replayed with the default heavy-create gate'

objects=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB}' AND name IN ('src', 'mv')")
echo "replayed objects present: $objects"
[[ $stored_populate -eq 0 && $dump_populate -eq 0 && $heavy_gate -eq 0 && $objects -eq 2 ]]
