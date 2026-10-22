#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# ntile over parallel replicas fails, see https://github.com/ClickHouse/ClickHouse/issues/112401

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'ranking over peers' "SELECT p, id, (rank() OVER w, dense_rank() OVER w, round(percent_rank() OVER w, 6), round(cume_dist() OVER w, 6)) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k)"
check 'ranking over rows' "SELECT p, id, (row_number() OVER w, rank() OVER w, dense_rank() OVER w, ntile(4) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
check 'lag and lead' "SELECT p, id, (lag(v, 3) OVER w, lead(v, 3) OVER w, lagInFrame(v, 2, -1) OVER f, leadInFrame(v, 2, -1) OVER f) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id), f AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
check 'nth first last value' "SELECT p, id, (nth_value(v, 5) OVER w, first_value(v) OVER w, last_value(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
check 'exponential time decayed' "SELECT p, id, round(exponentialTimeDecayedSum(5)(v, id) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING), 6) AS r FROM t"
check 'several aggregates one window' "SELECT p, id, (sum(v) OVER w, count() OVER w, min(v) OVER w, max(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
} | $CLICKHOUSE_CLIENT
