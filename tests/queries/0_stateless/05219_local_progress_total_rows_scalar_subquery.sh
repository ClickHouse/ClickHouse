#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A scalar subquery runs while the outer query is still being analysed, and clickhouse-local polls its
# interactive cancel callback every 100 ms for the whole time. That poll drains the accumulated
# progress, so a total published before the subquery has read anything must survive a poll that finds
# no read progress to go with it. The per-row sleep in PREWHERE holds the read inside the source for
# longer than one poll interval, which is what makes such a poll happen at all. The reported total is
# then the subquery's 5 rows plus the outer query's single row.
out=$($CLICKHOUSE_LOCAL --send_profile_events 0 -q "
    CREATE TABLE t_05219 (s String) ENGINE = MergeTree ORDER BY s;
    INSERT INTO t_05219 SELECT toString(number) FROM numbers(5);
    SELECT (SELECT count() FROM t_05219 PREWHERE NOT ignore(sleepEachRow(0.3), s)) FORMAT JSONEachRowWithProgress;
")
echo "$out" | grep -o -m1 '"total_rows_to_read":"6"'
