#!/usr/bin/env bash
# Tags: no-fasttest

# `--server_logs_file=-` sends the server logs and profile events to stdout. Like the default stderr
# sink and an explicit `--server_logs_file=<path>`, stdout is treated as a sink that can block (and
# so gets the lossy best-effort discipline for the trailing flush) only when it is a terminal, a FIFO
# or a socket. When it is redirected to an ordinary file or to a non-terminal character device such
# as `/dev/full`, the deferred `ProfileEvents` (held back by `--profile-events-delay-ms` and flushed
# after the query ends) must reach the sink through the plain, throwing write path: the events land
# in the file, and a write error is reported instead of being silently swallowed.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STDOUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_stdout_logs_sink.log"

rm -f "$STDOUT_FILE"

# The delay is longer than the query, so every event is deferred into the accumulator and written
# only by the trailing flush after the result has been delivered.
$CLICKHOUSE_CLIENT --server_logs_file=- \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "SELECT sum(number) FROM numbers(1000) FORMAT Null" > "$STDOUT_FILE"

if grep -q "ContextLock" "$STDOUT_FILE"
then
    echo "OK: the deferred profile events reached the regular file on stdout"
else
    echo "FAIL: the deferred profile events did not reach the regular file on stdout"
    cat "$STDOUT_FILE"
fi

rm -f "$STDOUT_FILE"

# A non-terminal character device cannot block either, so a write error on the trailing flush must
# surface as a failure of the query instead of being dropped as if the sink were a stuck terminal.
$CLICKHOUSE_CLIENT --server_logs_file=- \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "SELECT sum(number) FROM numbers(1000) FORMAT Null" > /dev/full 2> "${STDOUT_FILE}.err"
EXIT_CODE=$?

if [ "$EXIT_CODE" -ne 0 ] && grep -q "No space left on device" "${STDOUT_FILE}.err"
then
    echo "OK: the write error on /dev/full was reported"
else
    echo "FAIL: exit code ${EXIT_CODE}, the write error on /dev/full was not reported"
    cat "${STDOUT_FILE}.err"
fi

rm -f "${STDOUT_FILE}.err"
