#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-catalog, no-parallel-replicas

# A short `ATTACH` must retain the aligned firing schedule of a valid
# processing-time `WINDOW VIEW`: `next_fire_signal` has to stay a window upper bound
# (a multiple of the five-second window here), never the raw attach timestamp.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

SOURCE=window_view_attach_schedule_source
VIEW=window_view_attach_schedule_view

# Reading from a `WINDOW VIEW` also requires the experimental settings.
CLICKHOUSE_CLIENT_WV="${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0"

${CLICKHOUSE_CLIENT_WV} -q "DROP TABLE IF EXISTS ${VIEW}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${SOURCE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${SOURCE} (x UInt8) ENGINE = Memory"
${CLICKHOUSE_CLIENT_WV} -q "
    CREATE WINDOW VIEW ${VIEW} ENGINE = Memory
    AS SELECT count() AS c, tumble(now(), toIntervalSecond(5), 'UTC') AS w FROM ${SOURCE} GROUP BY w"

${CLICKHOUSE_CLIENT_WV} -q "DETACH TABLE ${VIEW}"

# Attach outside a five-second boundary. The old fallback started the firing loop at this
# unaligned timestamp, which can never match the stored window bounds.
while [ $(( $(date +%s) % 5 )) -eq 0 ]; do
    sleep 0.1
done

ATTACH_TIME=$(date +%s)
${CLICKHOUSE_CLIENT_WV} -q "ATTACH TABLE ${VIEW}"

LOGGER="StorageWindowView(${CLICKHOUSE_DATABASE}.${VIEW})"
QUERY="
    SELECT count(), countIf(signal % 5 = 0)
    FROM
    (
        SELECT toUInt32OrZero(extract(message, 'next fire signal: (\\d+)')) AS signal
        FROM system.text_log
        WHERE event_date >= yesterday()
          AND event_time >= toDateTime(${ATTACH_TIME})
          AND logger_name = '${LOGGER}'
          AND message LIKE '%next fire signal%'
    )"

# Wait for the firing loop to report its schedule at least once after the attach.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    result=$(${CLICKHOUSE_CLIENT} -q "${QUERY}")
    total=$(echo "${result}" | cut -f1)
    if [ "${total}" != "0" ]; then
        break
    fi
    sleep 0.5
done

# Every reported fire signal must be a five-second window upper bound.
echo "${result}" | awk '{ print ($1 > 0 && $1 == $2) ? "aligned" : "unaligned: " $0 }'

${CLICKHOUSE_CLIENT_WV} -q "DROP TABLE ${VIEW}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${SOURCE}"
