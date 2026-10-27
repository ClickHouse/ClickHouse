#!/usr/bin/env bash
# Tags: no-darwin
# Tag no-darwin: the `FileLog` engine is Linux-only.

# --dump-schema leaves the source as it was. In clickhouse-local, loading the metadata starts every table, but the dump
# must not refresh a refreshable materialized view, run a pending mutation or TTL merge, stream a `FileLog` into its view,
# or send the blocks that a `Distributed` table has queued. A database-less reference must not make the dump list the
# tables of an omitted `Remote` database either: that queries the server the database points to.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

DB="${CLICKHOUSE_DATABASE}"
# Absolute, because the clickhouse-local part runs in another working directory.
TMP_DIR=$(cd "$CLICKHOUSE_TMP" && pwd)
ERR_FILE="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"

echo '--- clickhouse-local: a dump runs no background work of the tables it loads ---'
LOCAL_PATH="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
LOCAL_DUMP_FILE="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.sql"
rm -rf "$LOCAL_PATH"
mkdir -p "$LOCAL_PATH/filelog"
# clickhouse-local accepts a `FileLog` path only inside its working directory.
cd "$LOCAL_PATH" || exit 1
# The view starts stopped here, so `dst` stays empty until a run refreshes it. Merges of `mut` and `mut_writable` and TTL
# merges of `ttl` are stopped for this run only, which leaves the mutations pending and the expired rows in place for the
# next run. `mut_writable` states `table_readonly = 0`, which the dump must not follow.
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --stop_refreshable_materialized_views_on_startup=1 --multiquery --query "
CREATE DATABASE d;
CREATE TABLE d.dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW d.rmv REFRESH EVERY 1 SECOND APPEND TO d.dst EMPTY AS SELECT number AS x FROM numbers(3);
CREATE TABLE d.mut (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO d.mut SELECT number, 0 FROM numbers(5);
SYSTEM STOP MERGES d.mut;
ALTER TABLE d.mut UPDATE y = 1 WHERE 1 SETTINGS mutations_sync = 0;
CREATE TABLE d.mut_writable (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS table_readonly = 0;
INSERT INTO d.mut_writable SELECT number, 0 FROM numbers(5);
SYSTEM STOP MERGES d.mut_writable;
ALTER TABLE d.mut_writable UPDATE y = 1 WHERE 1 SETTINGS mutations_sync = 0;
CREATE TABLE d.ttl (t DateTime, x UInt64) ENGINE = MergeTree ORDER BY x TTL t + INTERVAL 1 SECOND;
SYSTEM STOP TTL MERGES d.ttl;
INSERT INTO d.ttl SELECT now() - 100, number FROM numbers(5);
CREATE TABLE d.log (x UInt64) ENGINE = FileLog('${LOCAL_PATH}/filelog', 'JSONEachRow');
CREATE TABLE d.log_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW d.log_mv TO d.log_dst AS SELECT x FROM d.log;
"
# Written after the run that created the FileLog table, so only a later run can stream it.
echo '{"x": 1}' > "$LOCAL_PATH/filelog/a.jsonl"

# Every directory and file of the data path `$1` with its size, except the `status` file that each run rewrites.
snapshot()
{
    find "$1" -mindepth 1 ! -name status \( -type d -printf 'd %P\n' -o -type f -printf 'f %P %s\n' \) | LC_ALL=C sort
}
# The second dump also has `query_log` configured, whose table would be written into the data path.
QUERY_LOG_CONFIG="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_query_log.xml"
echo '<clickhouse><query_log><database>system</database><table>query_log</table>
<flush_interval_milliseconds>100</flush_interval_milliseconds></query_log></clickhouse>' > "$QUERY_LOG_CONFIG"
BEFORE=$(snapshot "$LOCAL_PATH")
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --dump-schema=d > "$LOCAL_DUMP_FILE" 2>"$ERR_FILE" \
    || echo "FAIL: dump failed: $(cat "$ERR_FILE")"
$CLICKHOUSE_LOCAL --config-file "$QUERY_LOG_CONFIG" --log_queries=1 --path "$LOCAL_PATH" --dump-schema=d \
    > "$LOCAL_DUMP_FILE" 2>"$ERR_FILE" || echo "FAIL: dump with query_log failed: $(cat "$ERR_FILE")"
echo "refreshable view dumped: $(grep -c '^CREATE MATERIALIZED VIEW d\.rmv ' "$LOCAL_DUMP_FILE")"
echo "FileLog view dumped: $(grep -c '^CREATE MATERIALIZED VIEW d\.log_mv ' "$LOCAL_DUMP_FILE")"
AFTER=$(snapshot "$LOCAL_PATH")
if [ "$BEFORE" = "$AFTER" ]; then
    echo 'data path unchanged by the dumps'
else
    echo 'FAIL: the dumps changed the data path:'
    diff <(echo "$BEFORE") <(echo "$AFTER")
fi
echo "rows in the target of the refreshable view: $($CLICKHOUSE_LOCAL --path "$LOCAL_PATH" \
    --stop_refreshable_materialized_views_on_startup=1 --query 'SELECT count() FROM d.dst')"
# Outside a dump the pending mutation runs: waiting for a later one waits for it too.
$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --stop_refreshable_materialized_views_on_startup=1 --multiquery --query "
ALTER TABLE d.mut UPDATE y = y WHERE 1 SETTINGS mutations_sync = 1;
SELECT 'mutated rows after a plain run', sum(y) FROM d.mut;
"
cd "$CUR_DIR" || exit 1
rm -rf "$LOCAL_PATH" "$LOCAL_DUMP_FILE" "$QUERY_LOG_CONFIG" "$ERR_FILE"

echo '--- clickhouse-local: a dump does not send the blocks that a Distributed table has queued ---'
DIST_PATH="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dist"
DIST_DUMP_FILE="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dist.sql"
DIST_CONFIG="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dist.xml"
rm -rf "$DIST_PATH"
# The shard is the server of the test, which clickhouse-local would take for itself on the port of its own `tcp_port`.
echo "<clickhouse><tcp_port>$((CLICKHOUSE_PORT_TCP + 1))</tcp_port><remote_servers><test_server><shard><replica>
<host>${CLICKHOUSE_HOST}</host><port>${CLICKHOUSE_PORT_TCP}</port></replica></shard></test_server></remote_servers></clickhouse>" \
    > "$DIST_CONFIG"
$CLICKHOUSE_CLIENT --query "CREATE TABLE dist_dst (x UInt64) ENGINE = MergeTree ORDER BY x"
# Sends are stopped, and the table does not flush when it is shut down, so the block stays queued for the next run.
$CLICKHOUSE_LOCAL --config-file "$DIST_CONFIG" --path "$DIST_PATH" --multiquery --query "
CREATE DATABASE d;
CREATE TABLE d.dist (x UInt64) ENGINE = Distributed(test_server, '${DB}', dist_dst, x) SETTINGS flush_on_detach = 0;
SYSTEM STOP DISTRIBUTED SENDS d.dist;
INSERT INTO d.dist SETTINGS distributed_foreground_insert = 0 VALUES (1), (2);
"
BEFORE=$(snapshot "$DIST_PATH")
$CLICKHOUSE_LOCAL --config-file "$DIST_CONFIG" --path "$DIST_PATH" --dump-schema=d > "$DIST_DUMP_FILE" 2>"$ERR_FILE" \
    || echo "FAIL: dump failed: $(cat "$ERR_FILE")"
echo "Distributed table dumped: $(grep -c '^CREATE TABLE d\.dist ' "$DIST_DUMP_FILE")"
AFTER=$(snapshot "$DIST_PATH")
if [ "$BEFORE" = "$AFTER" ]; then
    echo 'data path unchanged by the dump'
else
    echo 'FAIL: the dump changed the data path:'
    diff <(echo "$BEFORE") <(echo "$AFTER")
fi
echo "rows the shard received: $($CLICKHOUSE_CLIENT --query 'SELECT count() FROM dist_dst')"
$CLICKHOUSE_CLIENT --query "DROP TABLE dist_dst"
rm -rf "$DIST_PATH" "$DIST_DUMP_FILE" "$DIST_CONFIG" "$ERR_FILE"

echo '--- an omitted Remote database is not listed for a database-less reference ---'
REMOTE_SOURCE_DB="${DB}_fx_source"
REMOTE_DB="${DB}_fx_remote"
REMOTE_DUMP_FILE="${TMP_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}_remote.sql"
# A namesake of the parameterized view in any omitted database that can be listed would refuse the dump, so the views are
# named after the test database: other runs of this test are omitted databases of this dump.
PV="pv_${DB}"
READER="reader_${DB}"
$CLICKHOUSE_CLIENT --multiquery --query "
CREATE DATABASE ${REMOTE_SOURCE_DB};
CREATE TABLE ${REMOTE_SOURCE_DB}.marker_tbl (x UInt64) ENGINE = Memory;
CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', '${REMOTE_SOURCE_DB}');
CREATE VIEW ${PV} AS SELECT number FROM numbers(10) WHERE number = {p:UInt64};
CREATE VIEW ${READER} AS SELECT * FROM ${PV}(p = 1);
"
START=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp64Micro(now64(6))")
if $CLICKHOUSE_CLIENT --dump-schema="${DB}" > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "database-less reader dumped: $(grep -c "^CREATE VIEW ${DB}\.${READER} " "$REMOTE_DUMP_FILE")"
    # The Remote database could have a namesake too, which the dump no longer rules out.
    echo "Remote database named for the database-less reader: $(
        grep -F "${DB}.${READER} references ${PV} without a database" "$ERR_FILE" | grep -cF "${REMOTE_DB}")"
else
    echo "FAIL: dump failed: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "dump queries naming the Remote database: $($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND toUnixTimestamp64Micro(event_time_microseconds) >= ${START} AND type = 'QueryStart'
        AND query LIKE '%${REMOTE_DB}%' AND query NOT LIKE '%system.query_log%'")"
# The server's own background work can list the Remote database too, so only the queries that the queries of the dump
# started count: the Remote database sends its queries with the id of the query that needs them as `initial_query_id`.
echo "queries the Remote database sent to its server: $($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= ${START} AND type = 'QueryStart'
        AND NOT is_initial_query AND query LIKE '%${REMOTE_SOURCE_DB}%'
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND event_date >= yesterday()
                AND toUnixTimestamp64Micro(event_time_microseconds) >= ${START} AND is_initial_query)")"
$CLICKHOUSE_CLIENT --multiquery --query "
DROP DATABASE IF EXISTS ${REMOTE_DB};
DROP DATABASE IF EXISTS ${REMOTE_SOURCE_DB};
"
rm -f "$REMOTE_DUMP_FILE" "$ERR_FILE"
