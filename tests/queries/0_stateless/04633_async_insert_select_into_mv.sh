#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `INSERT INTO mv SELECT ...` takes the async queue route when the view writes into a MergeTree-family
# table. `EMPTY` and the yearly schedule keep the refreshable view from refreshing during the test.

# Under concurrent async inserts the adaptive busy timeout grows to the 5 seconds CI configures, and
# every queued insert here waits for its flush.
SETTINGS="async_insert = 1, wait_for_async_insert = 1, async_insert_select_as_async_insert = 1, max_threads = 1, max_insert_threads = 1, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_ms = 1"
CLICKHOUSE_CLIENT_DEBUG_LOGS=${CLICKHOUSE_CLIENT/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=debug}

function route() {
    ${CLICKHOUSE_CLIENT_DEBUG_LOGS} -q "$1 SETTINGS $SETTINGS" 2>&1 >/dev/null |
        grep -oE "INSERT \.\.\. SELECT (is eligible for the asynchronous insert queue route|will be executed synchronously \(reason: [^)]*\))" | head -1
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW mv_inner ENGINE = MergeTree ORDER BY n AS SELECT n FROM src;

    CREATE TABLE tgt (n UInt64, d UInt64 DEFAULT 42) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW mv_to TO tgt AS SELECT n FROM src;
    CREATE TABLE down (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE MATERIALIZED VIEW mv_down TO down AS SELECT n * 10 AS n FROM tgt;

    CREATE TABLE tgt_memory (n UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW mv_memory TO tgt_memory AS SELECT n FROM src;

    CREATE MATERIALIZED VIEW mv_refresh REFRESH EVERY 1 YEAR (n UInt64) ENGINE = MergeTree ORDER BY n EMPTY AS SELECT 1 AS n;
"

echo "view with an inner table"
route "INSERT INTO mv_inner SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(n)) FROM mv_inner"

echo "TO view, the target fills defaults and feeds its own view"
route "INSERT INTO mv_to SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray((n, d))) FROM tgt"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(n)) FROM down"

echo "SELECT reads the view target"
route "INSERT INTO mv_to SELECT n + 3 FROM tgt"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM tgt"

echo "Memory target"
route "INSERT INTO mv_memory SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM tgt_memory"

echo "refreshable view"
route "INSERT INTO mv_refresh SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM mv_refresh"
