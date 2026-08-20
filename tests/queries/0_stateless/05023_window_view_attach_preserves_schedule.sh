#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-catalog

# A short `ATTACH` must retain the aligned firing schedule of a valid
# processing-time `WINDOW VIEW`.

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

${CLICKHOUSE_CLIENT_WV} -q "ATTACH TABLE ${VIEW}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${SOURCE} VALUES (1)"

for _ in {1..100}; do
    count=$(${CLICKHOUSE_CLIENT_WV} -q "SELECT count() FROM ${VIEW}")
    if [ "${count}" = "1" ]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT_WV} -q "SELECT count() FROM ${VIEW}"

${CLICKHOUSE_CLIENT_WV} -q "DROP TABLE ${VIEW}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${SOURCE}"
