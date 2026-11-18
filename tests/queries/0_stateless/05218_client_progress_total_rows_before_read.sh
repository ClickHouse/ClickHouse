#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_05218;
    CREATE TABLE t_05218 (s String) ENGINE = MergeTree ORDER BY s;
    INSERT INTO t_05218 SELECT toString(number) FROM numbers(10);
"

# The total is known before the query starts reading, so it reaches the client carrying no read
# progress of its own, and it must still be forwarded to the output format. PREWHERE holds the read
# inside the source for longer than one interactive_delay tick, which is what makes that report
# observable; the sleep is per row, so the smallest randomized index_granularity still outlasts the
# tick. A read through the parallel replicas coordinator knows no total before execution, so the read
# has to stay local for there to be an early total at all.
out=$($CLICKHOUSE_CLIENT --send_profile_events 0 --query \
    "SELECT s FROM t_05218 PREWHERE NOT ignore(sleepEachRow(0.15), s)
     SETTINGS enable_parallel_replicas = 0 FORMAT JSONEachRowWithProgress")
echo "$out" | grep -o -m1 '"total_rows_to_read":"10"'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05218"
