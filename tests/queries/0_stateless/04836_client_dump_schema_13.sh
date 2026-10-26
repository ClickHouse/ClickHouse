#!/usr/bin/env bash

# --dump-schema cases from review: a view the analyzer inlines into a probed query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

# Under `analyzer_inline_views` the analyzer replaces a view with its query, so the probes of a materialized view that
# reads a view must not run the scalar subquery of that view.
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.src (x UInt64) ENGINE = Memory;
    CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS SELECT x, (SELECT sum(number) FROM numbers(1234567)) AS s FROM ${CLICKHOUSE_DATABASE}.src;
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv ENGINE = Memory AS SELECT x, s FROM ${CLICKHOUSE_DATABASE}.v GROUP BY x, s;"
$CLICKHOUSE_CLIENT --analyzer_inline_views=1 --dump-schema "$CLICKHOUSE_DATABASE" > /dev/null 2>&1
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'inlined view scalar subquery runs: ' || toString(count()) FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND query LIKE 'EXPLAIN QUERY TREE%' AND read_rows > 0"
