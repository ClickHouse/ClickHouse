#!/usr/bin/env bash
# Tags: no-fasttest

# An explicit `--server_logs_file` naming an ordinary file is not a sink that can block, so it must
# stay on the plain, throwing write path: the deferred `ProfileEvents` of the query (the ones held
# back by `--profile-events-delay-ms` and flushed after the query ends) have to reach the file, and
# a write error there has to be reported rather than silently swallowed by the best-effort
# discipline that only stuck terminals, FIFOs and other blocking sinks need.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOGS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_regular_logs_file.log"

rm -f "$LOGS_FILE"

# The delay is longer than the query, so every event is deferred into the accumulator and written
# only by the trailing flush after the result has been delivered.
$CLICKHOUSE_CLIENT --server_logs_file="$LOGS_FILE" \
    --print-profile-events --profile-events-delay-ms=600000 \
    --query "SELECT sum(number) FROM numbers(1000) FORMAT Null"

if grep -q "ContextLock" "$LOGS_FILE"
then
    echo "OK"
else
    echo "FAIL: the deferred profile events did not reach the regular log file"
    cat "$LOGS_FILE"
fi

rm -f "$LOGS_FILE"
