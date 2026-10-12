#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Window function results must not depend on how the sorted input is split into blocks.
# Every query runs with several block sizes, down to one row per block, and prints one
# fingerprint of its ordered result per block size; all fingerprints of a query must match.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (id UInt32, p UInt8, k Nullable(Int32), v Int64) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t SELECT
        number,
        multiIf(number < 1, 0, number < 6, 1, number < 150, 2, 3),
        if(number % 11 = 5, NULL, intDiv((number * 7) % 47, 4)),
        (number * 13) % 29
    FROM numbers(200);
"

function check()
{
    local name=$1
    local query=$2
    for block_size in 1 2 3 5 8 65505
    do
        echo -n "$name, block size $block_size: "
        $CLICKHOUSE_CLIENT --max_block_size "$block_size" -q "
            SELECT cityHash64(groupArray((p, id, r))) FROM ($query ORDER BY p, id)
        "
    done
}

# ORDER BY k, id is a total order, ORDER BY k alone leaves ties as peer groups.

check 'rows 3 preceding 2 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS r FROM t"
check 'rows 2 following 5 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS r FROM t"
check 'rows 5 preceding 2 preceding' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS r FROM t"
check 'rows unbounded preceding current row' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t"
check 'rows current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"
check 'rows 100 preceding 100 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) AS r FROM t"

check 'range default frame' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k) AS r FROM t"
check 'range 2 preceding 2 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS r FROM t"
check 'range 1 following 3 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS r FROM t"
check 'range current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"
check 'range desc 2 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'range nulls last 1 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"

check 'groups 1 preceding 1 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'groups 2 following 3 following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) AS r FROM t"
check 'groups 3 preceding 1 preceding' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS r FROM t"
check 'groups unbounded preceding current row' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t"
check 'groups current row unbounded following' "SELECT p, id, sum(v) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t"

check 'ranking over peers' "SELECT p, id, (rank() OVER w, dense_rank() OVER w, round(percent_rank() OVER w, 6), round(cume_dist() OVER w, 6)) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k)"
check 'ranking over rows' "SELECT p, id, (row_number() OVER w, rank() OVER w, dense_rank() OVER w, ntile(4) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
check 'lag and lead' "SELECT p, id, (lag(v, 3) OVER w, lead(v, 3) OVER w, lagInFrame(v, 2, -1) OVER f, leadInFrame(v, 2, -1) OVER f) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id), f AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
check 'nth first last value' "SELECT p, id, (nth_value(v, 5) OVER w, first_value(v) OVER w, last_value(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
check 'exponential time decayed' "SELECT p, id, round(exponentialTimeDecayedSum(5)(v, id) OVER (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING), 6) AS r FROM t"
check 'several aggregates one window' "SELECT p, id, (sum(v) OVER w, count() OVER w, min(v) OVER w, max(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"

check 'no partition by' "SELECT p, id, sum(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
check 'no order by' "SELECT p, id, sum(v) OVER (PARTITION BY p) AS r FROM t"
check 'single row partitions' "SELECT p, id, sum(v) OVER (PARTITION BY id ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t"
