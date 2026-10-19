#!/usr/bin/env bash
# Tags: no-fasttest, no-flaky-check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A unique secret marker so we only look at log lines produced by this test (parallel-safe).
secret="find_me_${CLICKHOUSE_DATABASE}_TOPSECRET"

# Send the secret as the value of sensitive query-string parameters. The server logs
# "Request URI: ..." at trace level; the value must be redacted there.
#
# The query body is always "SELECT 1" and never references the bound parameter, so the only
# place the secret could surface is the URI log. (If the query echoed {secret_key:String},
# the substituted value would legitimately appear in the executeQuery log, which is a
# different code path unrelated to URI masking.) Each parameter uses a distinct suffix so the
# per-parameter assertions below cannot be confused with one another.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_secret_key=${secret}_skey" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&password=${secret}_pw" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&sig=${secret}_sig" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1
# Percent-encoded sensitive name: the server decodes "pass%77ord" to "password" before using it,
# so masking must too, or the value leaks. --globoff stops curl treating the literal as a glob.
${CLICKHOUSE_CURL} -sS --globoff "${CLICKHOUSE_URL}&pass%77ord=${secret}_enc" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1

# A non-sensitive parameter must NOT be redacted. Issued last on purpose (see the loop below).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_visible=${secret}_visible" -d "SELECT 1 FORMAT Null" >/dev/null 2>&1

# The "Request URI" trace entry is written asynchronously, after the HTTP response is sent
# (https://github.com/ClickHouse/ClickHouse/issues/84364), so a single FLUSH LOGS races the
# log write. Retry FLUSH until both positive-control rows have landed: the masked
# param_secret_key line and the non-sensitive param_visible line (issued last, so all earlier
# Request URI lines are flushed by then too). enable_parallel_replicas=0 keeps these
# system.text_log reads local (CI randomization may otherwise route them to a cluster).
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS text_log"
    landed=$(${CLICKHOUSE_CLIENT} --query="
        SELECT
            countIf(message LIKE '%Request URI%' AND message LIKE '%param_secret_key=[HIDDEN]%') > 0
            AND countIf(message LIKE '%Request URI%' AND message LIKE '%param_visible=${secret}\\_visible%') > 0
        FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
        SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0")
    [ "$landed" = "1" ] && break
    sleep 0.5
done

echo "--- secret values must NOT appear in text_log ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
      AND message LIKE '%${secret}\\_skey%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
      AND (message LIKE '%${secret}\\_pw%' OR message LIKE '%${secret}\\_sig%')
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- a percent-encoded sensitive name (pass%77ord -> password) must also be redacted ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
      AND message LIKE '%${secret}\\_enc%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- the Request URI line for the secret param must be logged with [HIDDEN] ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
      AND message LIKE '%Request URI%'
      AND message LIKE '%param_secret_key=[HIDDEN]%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"

echo "--- a non-sensitive parameter value is still visible ---"
${CLICKHOUSE_CLIENT} --query="
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
      AND message LIKE '%Request URI%'
      AND message LIKE '%param_visible=${secret}\\_visible%'
    SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0"
