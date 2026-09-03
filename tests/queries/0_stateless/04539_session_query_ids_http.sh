#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique per run so that a rerun in the same database does not inherit a still-alive named session.
SESSION_ID="04539_session_query_ids_${CLICKHOUSE_DATABASE}_$$_${RANDOM}"
SESSION_URL="${CLICKHOUSE_URL}&session_id=${SESSION_ID}"

echo '-- named session: ids persist across requests'
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT 'marker 1' FORMAT Null"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT 'marker 2' FORMAT Null"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT count(), countIf(query_id = queryID()) FROM system.session_query_ids"

echo '-- TRUNCATE resets the contents'
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "TRUNCATE TABLE system.session_query_ids"
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT count() FROM system.session_query_ids"

echo '-- the sequence counter is not reset by TRUNCATE'
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT min(sequence_number) > 1 FROM system.session_query_ids"

echo '-- a query that fails to parse is recorded too'
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT 1 FORMAT" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" -d "SELECT count() FROM system.session_query_ids"

echo '-- session_query_ids_history_size = 0 disables recording'
DISABLED_SESSION_URL="${CLICKHOUSE_URL}&session_id=${SESSION_ID}_disabled"
${CLICKHOUSE_CURL} -sS "${DISABLED_SESSION_URL}&session_query_ids_history_size=0" -d "SELECT 'not recorded' FORMAT Null"
${CLICKHOUSE_CURL} -sS "${DISABLED_SESSION_URL}&session_query_ids_history_size=0" -d "SELECT count() FROM system.session_query_ids"
${CLICKHOUSE_CURL} -sS "${DISABLED_SESSION_URL}&session_query_ids_history_size=10" -d "SELECT count(), countIf(query_id = queryID()) FROM system.session_query_ids"

echo '-- a plain HTTP request without session_id sees only its own query'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT count(), countIf(query_id = queryID()) FROM system.session_query_ids"
