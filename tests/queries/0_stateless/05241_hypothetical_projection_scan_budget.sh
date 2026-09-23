#!/usr/bin/env bash
# past max_rows_to_scan the estimate reads a sample of granules and models each part from it; the twin
# table with the projections materialized is the ground truth the sampled span has to contain

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1"

# b is spread evenly over the parent order, c follows it, which leaves key space no sampled granule covers
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_scan; DROP TABLE IF EXISTS t_real_scan;
    CREATE TABLE t_scan (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    CREATE TABLE t_real_scan AS t_scan;
    ALTER TABLE t_real_scan ADD PROJECTION p_b (SELECT a, b, c ORDER BY b);
    ALTER TABLE t_real_scan ADD PROJECTION p_c (SELECT a, b, c ORDER BY c);
    INSERT INTO t_scan SELECT number, cityHash64(number) % 1000, intDiv(number, 100) FROM numbers(100000);
    INSERT INTO t_real_scan SELECT number, cityHash64(number) % 1000, intDiv(number, 100) FROM numbers(100000);
"

# prints whether the estimate was sampled and whether the real read falls inside the span it reports
check()
{
    local projection="$1" key="$2" where="$3" options="$4"
    local out est span low high real
    out=$($CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION ${projection} ON t_scan (SELECT a, b, c ORDER BY ${key});
        EXPLAIN WHATIF ${options} SELECT count() FROM t_scan WHERE ${where} SETTINGS ${PIN};
        SELECT '--- real ---';
        EXPLAIN indexes = 1 SELECT count() FROM t_real_scan WHERE ${where}
            SETTINGS ${PIN}, preferred_optimize_projection_name = '${projection}';
    ")
    est=$(sed -n '1,/^--- real ---$/p' <<< "$out" | grep -E '^\s+marks:' | tail -1 | awk '{print $2}')
    # no span means every layout came out the same, so the estimate is the whole of it
    span=$(grep -oE 'marks_span:\s+[0-9]+ to [0-9]+' <<< "$out" | grep -oE '[0-9]+ to [0-9]+')
    low=$(awk '{print $1}' <<< "${span:-$est to $est}")
    high=$(awk '{print $3}' <<< "${span:-$est to $est}")
    real=$(sed -n '/^--- real ---$/,$p' <<< "$out" | grep -oE 'Granules: [0-9]+$' | head -1 | awk '{print $2}')
    local sampled
    sampled=$(grep -oE 'sampled_marks:\s+[0-9]+ / [0-9]+' <<< "$out" | awk '{print ($2 < $4)}')
    echo "${where}: sampled ${sampled}, real inside the span $(( real >= low && real <= high ? 1 : 0 ))"
}

echo "--- the whole part is read when it fits the budget, and the estimate is exact ---"
check p_b b "b < 100" "max_rows_to_scan = 0"

echo "--- a key spread over the parent order, sampled ---"
check p_b b "b < 100" "max_rows_to_scan = 5000"
check p_b b "b = 7" "max_rows_to_scan = 5000"

echo "--- a key that follows the parent order, sampled ---"
check p_c c "c >= 300 AND c < 310" "max_rows_to_scan = 5000"
check p_c c "c = 42" "max_rows_to_scan = 5000"

# the query is pruned under the limit while the estimate needs the whole part: a read limit used to end
# the estimate there, now it only lowers the budget
echo "--- max_rows_to_read below the whole-part scan ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_scan (SELECT a, b, c ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_scan WHERE a < 1000 AND b < 100 SETTINGS ${PIN}, max_rows_to_read = 30000;
" | grep -E '^\s+(status|source|empirical_status):' | awk '{$1=$1; print}'
