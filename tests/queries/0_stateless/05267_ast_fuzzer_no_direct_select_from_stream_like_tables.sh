#!/usr/bin/env bash
# Queries run by the server-side AST fuzzer (`ast_fuzzer_runs`) must not read a stream-like table directly, even when
# the original query allows it: such a read consumes the table's messages, which may belong to another test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
PROFILE="profile_direct_select_${CLICKHOUSE_DATABASE}"
mkdir -p "$DATA_DIR"
printf '1\n2\n3\n' > "$DATA_DIR/a.csv"

$CLICKHOUSE_CLIENT -m -q "
SET ast_fuzzer_runs = 0;
DROP SETTINGS PROFILE IF EXISTS $PROFILE;
CREATE SETTINGS PROFILE $PROFILE SETTINGS stream_like_engine_allow_direct_select = 1;
CREATE TABLE file_log (k UInt64) ENGINE = FileLog('$DATA_DIR/', 'CSV');
"

# The query allows direct reads through a settings profile and through the setting itself, and reads the rows.
# Sent over HTTP: the native client applies a query's own `SETTINGS` locally, where no settings profile exists.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT k FROM file_log ORDER BY k SETTINGS profile = '$PROFILE', stream_like_engine_allow_direct_select = 1, ast_fuzzer_runs = 30"

# The query's own `query_log` entry can be written after the HTTP response; wait for it. Its fuzzed copies are logged
# before it.
for _ in $(seq 1 60); do
    count=$($CLICKHOUSE_CLIENT -m -q "
        SET ast_fuzzer_runs = 0;
        SYSTEM FLUSH LOGS query_log;
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
            AND type = 'QueryFinish' AND query LIKE 'SELECT k FROM file_log %'")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

# Every copy the fuzzer ran for it must have had direct reads disabled.
$CLICKHOUSE_CLIENT -m -q "
SET ast_fuzzer_runs = 0;
SELECT count() > 0, countIf(Settings['stream_like_engine_allow_direct_select'] != '0') = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND is_internal;
DROP TABLE file_log;
DROP SETTINGS PROFILE $PROFILE;
"

rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
