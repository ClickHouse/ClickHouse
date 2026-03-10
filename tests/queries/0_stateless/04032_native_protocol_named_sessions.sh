#!/usr/bin/env bash
# Tags: long

# Native protocol equivalent of 00463_long_sessions_in_http_interface.
# Tests named sessions over the TCP protocol (session_id in the handshake addendum).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Using non-existent session with the session_check flag will throw exception:"
${CLICKHOUSE_CLIENT} --session_id="nonexistent_04032" --session_check -q "SELECT 1" 2>&1 | grep -c -F 'SESSION_NOT_FOUND'

echo "Using non-existent session without the session_check flag will create a new session:"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_1" -q "SELECT 1"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_1" -q "SELECT 1"

echo "Valid session_timeout values are accepted:"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_3" --session_timeout=0 -q "SELECT 1"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_4" --session_timeout=3600 -q "SELECT 1"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_5" --session_timeout=60 -q "SELECT 1"

echo "Sessions are local per user:"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS test_04032"
${CLICKHOUSE_CLIENT} -q "CREATE USER test_04032"
${CLICKHOUSE_CLIENT} -q "GRANT CURRENT GRANTS(ALL ON *.*) TO test_04032"

${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_6" --session_timeout=600 -q "CREATE TEMPORARY TABLE t (s String)"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_6" -q "INSERT INTO t VALUES ('Hello')"

# Different user with same session_id should not find the first user's session
${CLICKHOUSE_CLIENT} --user=test_04032 --session_id="${CLICKHOUSE_DATABASE}_04032_6" --session_check -q "SELECT 1" 2>&1 | grep -c -F 'SESSION_NOT_FOUND'

# Different user creates their own session with same session_id
${CLICKHOUSE_CLIENT} --user=test_04032 --session_id="${CLICKHOUSE_DATABASE}_04032_6" --session_timeout=600 -q "CREATE TEMPORARY TABLE t (s String)"
${CLICKHOUSE_CLIENT} --user=test_04032 --session_id="${CLICKHOUSE_DATABASE}_04032_6" -q "INSERT INTO t VALUES ('World')"

# Each user sees their own temp table
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_6" -q "SELECT * FROM t"
${CLICKHOUSE_CLIENT} --user=test_04032 --session_id="${CLICKHOUSE_DATABASE}_04032_6" -q "SELECT * FROM t"

${CLICKHOUSE_CLIENT} -q "DROP USER test_04032";

echo "The temporary tables created in a session are not accessible without entering this session:"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t" 2>&1 | grep -c -F 'UNKNOWN_TABLE'

echo "A session successfully expires after a timeout:"
# Use a retry loop like the HTTP test to handle timing variability
while true
do
    (
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_7" --session_timeout=1 -q "SELECT 1"
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_7" --session_check -q "SELECT 1"
        sleep 3
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_7" --session_check -q "SELECT 1" 2>&1 | grep -c -F 'SESSION_NOT_FOUND'
    ) | tr -d '\n' | grep -F '111' && break || sleep 1
done

echo "A session successfully expires after a timeout and the session's temporary table shadows the permanent table:"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04032; CREATE TABLE t_04032 (s String) ENGINE = Memory; INSERT INTO t_04032 VALUES ('World');"
while true
do
    (
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_8" --session_timeout=1 -q "CREATE TEMPORARY TABLE t_04032 (s String)"
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_8" -q "INSERT INTO t_04032 VALUES ('Hello')"
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_8" -q "SELECT * FROM t_04032"
        sleep 3
        ${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_8" -q "SELECT * FROM t_04032"
    ) | tr -d '\n' | grep -F 'HelloWorld' && break || sleep 1
done
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_04032"

echo "Settings persist across reconnects in a named session:"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_10" --session_timeout=60 -q "SET max_threads=42"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_10" -q "SELECT getSetting('max_threads')"

echo "A session cannot be used by concurrent connections:"
${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_9" --session_timeout=60 --max_rows_to_read=0 --query_id="${CLICKHOUSE_DATABASE}_04032_9" -q "SELECT count() FROM system.numbers" >/dev/null 2>&1 &

# Wait for the query to start
while true
do
    ${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.processes WHERE query_id = '${CLICKHOUSE_DATABASE}_04032_9'" | grep -F '1' && break || sleep 1
done

${CLICKHOUSE_CLIENT} --session_id="${CLICKHOUSE_DATABASE}_04032_9" -q "SELECT 1" 2>&1 | grep -c -F 'SESSION_IS_LOCKED'
${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_04032_9' SYNC FORMAT Null";
wait
