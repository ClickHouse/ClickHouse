#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A refresh period this far out yields a delay whose microsecond value overflows when the wait
# widens it to nanoseconds. Deliberately no assertion on the refresh's scheduled state: that is
# timing dependent here, while the sanitizer abort is not.
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv_huge_delay
        REFRESH AFTER 20000000000 SECOND
        APPEND (x Int64) ENGINE = Memory AS SELECT 1 AS x;
"

sleep 3

${CLICKHOUSE_CLIENT} --query "SELECT 'alive', 1"
