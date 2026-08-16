#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="
    SELECT k, count()
    FROM
    (
        SELECT toUInt256(number % 3) AS k
        FROM numbers(100000)
    )
    GROUP BY k
    ORDER BY k
    SETTINGS enable_software_prefetch_in_aggregation = 1"

# The trace message is emitted by the selected aggregation method, unlike the
# `EXPLAIN` key list, which reflects only the `GROUP BY` expression.
$CLICKHOUSE_CLIENT --send_logs_level=trace -q "$query" 2>&1 \
    | grep -oE 'Aggregation method: [a-z_0-9]+' | sort -u
$CLICKHOUSE_CLIENT -q "$query"
