#!/usr/bin/env bash
# every case has a twin table with the same projection materialized, the estimate must match what it reads

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# count() must reach the read step and the real projection must be allowed to win
PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real;
    CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real AS t_est;
    ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    ALTER TABLE t_real ADD PROJECTION p_ba (SELECT a, b, v ORDER BY (b, a));
    ALTER TABLE t_real ADD PROJECTION p_c (SELECT a, b, v, b * 2 AS c ORDER BY c);
    ALTER TABLE t_real ADD PROJECTION p_idx INDEX b TYPE basic;
    SYSTEM STOP MERGES t_est; SYSTEM STOP MERGES t_real;
    -- three parts, b = a % 100 so every b value is one granule per part once sorted
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(300, 300);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(600, 400);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(300, 300);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(600, 400);
    -- WITH SETTINGS granularity overrides need an adaptive-granularity parent
    DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;
    CREATE TABLE t_est_g (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_g AS t_est_g;
    ALTER TABLE t_real_g ADD PROJECTION p_g (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50);
    -- kept larger than the other tables: the not-chosen case needs the projection to read several granules
    INSERT INTO t_est_g SELECT number, number % 100, number FROM numbers(10000);
    INSERT INTO t_real_g SELECT number, number % 100, number FROM numbers(10000);
"

# the read-step header holds the final granule count, the per-index lines vary by build
compare()
{
    local projection_name="$1" projection_body="$2" query="$3" est="${4:-t_est}" real="${5:-t_real}"
    # both halves in one session: a client start costs more than everything the queries do
    local out
    out=$($CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION ${projection_name} ON ${est} ${projection_body};
        EXPLAIN WHATIF ${query/TABLE/$est} SETTINGS ${PIN};
        SELECT '--- real ---';
        EXPLAIN indexes = 1 ${query/TABLE/$real} SETTINGS ${PIN}, preferred_optimize_projection_name = '${projection_name}';
    ")
    echo "hypothetical:"
    sed -n '1,/^--- real ---$/p' <<< "$out" | grep -E '^\s+(status|marks|rows|read_ratio|verdict|reason|source):' | awk '{$1=$1; print}'
    echo "real:"
    sed -n '/^--- real ---$/,$p' <<< "$out" | grep -oE 'ReadFromMergeTree \([^)]*\)|Granules: [0-9]+$'
}

echo "--- point query on the projection key, three parts ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- range on the projection key ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b >= 10 AND b < 20"

echo "--- composite key, leading column ---"
compare p_ba "(SELECT a, b, v ORDER BY (b, a))" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- base PK already prunes to one granule, the projection cannot beat it ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE a < 100 AND b = 42"

# a tie plus an ORDER BY this projection cannot serve, the fourth state of the tie-break reason
echo "--- a tie with an ORDER BY the projection order cannot serve ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE a < 100 AND b = 42 ORDER BY v SETTINGS ${PIN};
" | grep -E '^\s+(verdict|reason):' | awk '{$1=$1; print}'

echo "--- key over a computed expression ---"
compare p_c "(SELECT a, b, v, b * 2 AS c ORDER BY c)" "SELECT count() FROM TABLE WHERE b * 2 = 84"

echo "--- the projection's own index_granularity is used ---"
compare p_g "(SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50)" "SELECT count() FROM TABLE WHERE b = 42" t_est_g t_real_g

echo "--- no filter, the projection serves the ORDER BY ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT a, b, v FROM TABLE ORDER BY b"
# every setting the planner's read-in-order gate is built from must switch the tie-break off
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est ORDER BY b SETTINGS ${PIN}, optimize_read_in_order = 0;
    EXPLAIN WHATIF SELECT a, b, v FROM t_est ORDER BY b SETTINGS ${PIN}, query_plan_enable_optimizations = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'

echo "--- the INDEX form, which the optimizer serves the read from ---"
compare p_idx "INDEX b TYPE basic" "SELECT count() FROM TABLE WHERE b = 42"

# the projection's own granularity makes it read more than a base read the primary key already pruned
echo "--- a projection that reads more marks than the base table is not chosen ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_g ON t_est_g (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_g WHERE a < 100 AND b = 42 SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT a, b, v FROM t_real_g WHERE a < 100 AND b = 42 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_g'" \
    | grep -oE 'ReadFromMergeTree \(p_g\)' || echo "real: read from the base table"

# every block size the writer could have been handed lays this part out the same way, so a tie here
# is still called
echo "--- a tie on an adaptive-granularity part is called ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_a;
    CREATE TABLE t_est_a (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO t_est_a SELECT number, number % 100, number FROM numbers(250);
    CREATE HYPOTHETICAL PROJECTION p_a ON t_est_a (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_a WHERE b >= 0 SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'

# the ORDER BY tie-break is decided by the same mark comparison
echo "--- the ORDER BY tie-break on the same tie ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_a ON t_est_a (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_a ORDER BY b SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'

# the read step of such a baseline carries the real projection's metadata, not the table's
echo "--- a baseline already served by a real projection is reported as such ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_served;
    CREATE TABLE t_served (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    ALTER TABLE t_served ADD PROJECTION p_ab (SELECT a, b ORDER BY b);
    INSERT INTO t_served SELECT number, number % 100, number FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p_c ON t_served (SELECT a, b, c ORDER BY c);
    EXPLAIN WHATIF SELECT a, b FROM t_served WHERE b = 42 SETTINGS ${PIN};
" | grep -E '^\s+(status|reason):' | awk '{$1=$1; print}'
