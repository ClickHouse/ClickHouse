#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TRACE_MARKER='Stack trace (when copying this message'

# A client that asked for server logs receives the error log line, which carries the trace.
echo -n 'log reader: '
$CLICKHOUSE_LOCAL --send_logs_level=error -q "SELECT throwIf(1)" 2>&1 \
    | grep -qF "$TRACE_MARKER" && echo 1 || echo 0

# A configured `query_log` reads the trace out of its `stack_trace` column, with the logger silent.
test_dir="${CLICKHOUSE_TMP}/05223_${CLICKHOUSE_DATABASE}"
rm -rf "${test_dir}"
mkdir -p "${test_dir}/data" "${test_dir}/tmp" "${test_dir}/user_files"
cat > "${test_dir}/config.xml" <<EOF
<clickhouse>
    <path>${test_dir}/data/</path>
    <tmp_path>${test_dir}/tmp/</tmp_path>
    <user_files_path>${test_dir}/user_files/</user_files_path>
    <logger>
        <level>none</level>
        <console>false</console>
    </logger>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY event_time</engine>
    </query_log>
</clickhouse>
EOF

echo -n 'query_log reader: '
$CLICKHOUSE_LOCAL --config-file="${test_dir}/config.xml" --ignore-error -n -q "
    SELECT throwIf(1);
    SYSTEM FLUSH LOGS query_log;
    SELECT length(stack_trace) > 100 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
        ORDER BY event_time_microseconds DESC LIMIT 1;
" 2>/dev/null

rm -rf "${test_dir}"

# With no reader the exception is still reported to the user, and no trace is computed for it.
echo -n 'no reader, error still reported: '
$CLICKHOUSE_LOCAL -q "SELECT throwIf(1)" 2>&1 \
    | grep -qF 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' && echo 1 || echo 0

echo -n 'no reader, no stack trace: '
$CLICKHOUSE_LOCAL -q "SELECT throwIf(1)" 2>&1 \
    | grep -qF "$TRACE_MARKER" && echo 1 || echo 0

# `--stacktrace` is the client's own print path, independent of the log and `query_log` readers.
echo -n 'client --stacktrace: '
$CLICKHOUSE_LOCAL --stacktrace -q "SELECT throwIf(1)" 2>&1 \
    | grep -qF "$TRACE_MARKER" && echo 1 || echo 0

# The logger alone is a reader: no `query_log`, and the client asked for no server logs.
echo -n 'console logger reader: '
$CLICKHOUSE_LOCAL --logger.console --log-level=error --send_logs_level=none -q "SELECT throwIf(1)" 2>&1 \
    | grep -qF "$TRACE_MARKER" && echo 1 || echo 0

# The same predicate guards the error-before-start site, which no other arm reaches.
echo -n 'before start, client log reader: '
$CLICKHOUSE_LOCAL --send_logs_level=error -q "SELECT * FROM does_not_exist_05223" 2>&1 \
    | grep -qF "$TRACE_MARKER" && echo 1 || echo 0
