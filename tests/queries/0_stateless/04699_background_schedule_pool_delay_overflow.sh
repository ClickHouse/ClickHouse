#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database, memory-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A refresh period this far out yields a delay whose microsecond value overflows when the wait
# widens it to nanoseconds. The primary oracle is the sanitizer abort, so this test only proves the
# fix in a sanitizer build; the assertions below are the build-independent part.
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv_huge_delay
        REFRESH AFTER 20000000000 SECOND
        APPEND (x Int64) ENGINE = Memory AS SELECT 1 AS x;
"

sleep 3

# The task must be parked, not dropped: a delay that wrapped into a past deadline would fire.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.background_schedule_pool
    WHERE pool = 'schedule' AND log_name = 'RefreshSched'
      AND database = currentDatabase() AND delayed = 1;

    SELECT count() FROM ${CLICKHOUSE_DATABASE}.mv_huge_delay;
"
