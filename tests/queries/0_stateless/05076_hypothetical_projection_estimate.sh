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
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000, 3000);
    INSERT INTO t_est SELECT number, number % 100, number FROM numbers(6000, 4000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000, 3000);
    INSERT INTO t_real SELECT number, number % 100, number FROM numbers(6000, 4000);
    -- WITH SETTINGS granularity overrides need an adaptive-granularity parent
    DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;
    CREATE TABLE t_est_g (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_g AS t_est_g;
    ALTER TABLE t_real_g ADD PROJECTION p_g (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50);
    INSERT INTO t_est_g SELECT number, number % 100, number FROM numbers(10000);
    INSERT INTO t_real_g SELECT number, number % 100, number FROM numbers(10000);
"

# the read-step header holds the final granule count, the per-index lines vary by build
compare()
{
    local projection_name="$1" projection_body="$2" query="$3" est="${4:-t_est}" real="${5:-t_real}"
    echo "hypothetical:"
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION ${projection_name} ON ${est} ${projection_body};
        EXPLAIN WHATIF ${query/TABLE/$est} SETTINGS ${PIN};
    " | grep -E '^\s+(status|marks|rows|read_ratio|verdict|reason|source):' | awk '{$1=$1; print}'
    echo "real:"
    $CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 ${query/TABLE/$real} SETTINGS ${PIN}, preferred_optimize_projection_name = '${projection_name}'" \
        | grep -oE 'ReadFromMergeTree \([^)]*\)|Granules: [0-9]+$'
}

echo "--- point query on the projection key, three parts ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- range on the projection key ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE b >= 10 AND b < 20"

echo "--- composite key, leading column ---"
compare p_ba "(SELECT a, b, v ORDER BY (b, a))" "SELECT count() FROM TABLE WHERE b = 42"

echo "--- base PK already prunes to one granule, the projection cannot beat it ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT count() FROM TABLE WHERE a < 100 AND b = 42"

echo "--- key over a computed expression ---"
compare p_c "(SELECT a, b, v, b * 2 AS c ORDER BY c)" "SELECT count() FROM TABLE WHERE b * 2 = 84"

echo "--- the projection's own index_granularity is used ---"
compare p_g "(SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50)" "SELECT count() FROM TABLE WHERE b = 42" t_est_g t_real_g

echo "--- no filter, the projection serves the ORDER BY ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT a, b, v FROM TABLE ORDER BY b"
# every setting the planner's read-in-order gate is built from must switch the tie-break off
for off in "optimize_read_in_order = 0" "query_plan_read_in_order = 0" "query_plan_enable_optimizations = 0"; do
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
        EXPLAIN WHATIF SELECT a, b, v FROM t_est ORDER BY b SETTINGS ${PIN}, ${off};
    " | grep -E '^\s+reason:' | awk '{$1=$1; print}'
done

echo "--- the INDEX form, which the optimizer serves the read from ---"
compare p_idx "INDEX b TYPE basic" "SELECT count() FROM TABLE WHERE b = 42"

# a remainder opens a granule of its own, as in the writer
echo "--- a row count that is not a multiple of the granule ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_r; DROP TABLE IF EXISTS t_real_r;
    CREATE TABLE t_est_r (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_r AS t_est_r;
    ALTER TABLE t_real_r ADD PROJECTION p_r (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est_r SELECT number, number % 25, number FROM numbers(250);
    INSERT INTO t_real_r SELECT number, number % 25, number FROM numbers(250);
    SELECT 'real projection part marks:', marks FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_real_r' AND active;
"
compare p_r "(SELECT a, b, v ORDER BY b)" "SELECT a, b, v FROM TABLE WHERE b = 7" t_est_r t_real_r
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_r; DROP TABLE IF EXISTS t_real_r;"

# a Compact part reports the whole part's size for every column, so the bytes must come from the scan;
# such a part carries an adaptive granularity, which the constant model can miss by a granule
echo "--- Compact parts, where a wide column the projection does not store must not inflate its marks ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_c; DROP TABLE IF EXISTS t_real_c;
    CREATE TABLE t_est_c (id UInt64, b UInt64, v UInt64, payload String) ENGINE = MergeTree ORDER BY id
        SETTINGS index_granularity = 8192, index_granularity_bytes = 10000, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_c AS t_est_c;
    ALTER TABLE t_real_c ADD PROJECTION p_c (SELECT id, b, v ORDER BY b);
    -- 2050 rows leave a remainder below half a granule, which the compact writer folds into the previous mark
    INSERT INTO t_est_c SELECT number, number % 1000, number, repeat('x', 400) FROM numbers(2050);
    INSERT INTO t_real_c SELECT number, number % 1000, number, repeat('x', 400) FROM numbers(2050);
    SELECT 'real projection part marks:', marks FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_real_c' AND active;
"
compare p_c "(SELECT id, b, v ORDER BY b)" "SELECT sum(v) FROM TABLE WHERE b >= 0" t_est_c t_real_c
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_c; DROP TABLE IF EXISTS t_real_c;"

# the projection's own granularity makes it read more than a base read the primary key already pruned
echo "--- a projection that reads more marks than the base table is not chosen ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_g ON t_est_g (SELECT a, b, v ORDER BY b) WITH SETTINGS (index_granularity = 50);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_g WHERE a < 100 AND b = 42 SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT a, b, v FROM t_real_g WHERE a < 100 AND b = 42 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_g'" \
    | grep -oE 'ReadFromMergeTree \(p_g\)' || echo "real: read from the base table"

# on an adaptive part the constant model can miss a granule, so a decision that close is not claimed
echo "--- a near-tie on an adaptive-granularity part is not called ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_a;
    CREATE TABLE t_est_a (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 10485760, use_const_adaptive_granularity = 0,
                 min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO t_est_a SELECT number, number % 100, number FROM numbers(250);
    CREATE HYPOTHETICAL PROJECTION p_a ON t_est_a (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_a WHERE b >= 0 SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'

# the ORDER BY tie-break is decided by the same mark comparison, so it is uncertain in the same window
echo "--- an ORDER BY tie-break inside the margin is not called either ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_a ON t_est_a (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a, b, v FROM t_est_a ORDER BY b SETTINGS ${PIN};
" | grep -E '^\s+(marks|read_ratio|verdict|reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_a;"

# the read step of such a baseline carries the real projection's metadata, not the table's
echo "--- a baseline already served by a real projection is reported as such ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_served;
    CREATE TABLE t_served (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    ALTER TABLE t_served ADD PROJECTION p_ab (SELECT a, b ORDER BY b);
    INSERT INTO t_served SELECT number, number % 100, number FROM numbers(10000);
    CREATE HYPOTHETICAL PROJECTION p_c ON t_served (SELECT a, b, c ORDER BY c);
    EXPLAIN WHATIF SELECT a, b FROM t_served WHERE b = 42 SETTINGS ${PIN};
" | grep -E '^\s+(status|reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_served;"

# `required_columns` omits a subcolumn whose physical column is present, the key expression still needs it
echo "--- a sort key over a subcolumn ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_s; DROP TABLE IF EXISTS t_real_s;
    CREATE TABLE t_est_s (t Tuple(x UInt64, y UInt64), v UInt64) ENGINE = MergeTree ORDER BY v
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_s AS t_est_s;
    ALTER TABLE t_real_s ADD PROJECTION p_s (SELECT t, v ORDER BY t.x);
    INSERT INTO t_est_s SELECT (number % 100, number), number FROM numbers(10000);
    INSERT INTO t_real_s SELECT (number % 100, number), number FROM numbers(10000);
"
compare p_s "(SELECT t, v ORDER BY t.x)" "SELECT t, v FROM TABLE WHERE t.x = 42" t_est_s t_real_s
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_s; DROP TABLE IF EXISTS t_real_s;"

echo "--- not applicable cases ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_key ON t_est (SELECT a, b, v ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_agg ON t_est (SELECT b, sum(v) GROUP BY b);
    CREATE HYPOTHETICAL PROJECTION p_where ON t_est (SELECT a, b, v WHERE b < 50 ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_nocol ON t_est (SELECT a, b ORDER BY b);
    CREATE HYPOTHETICAL PROJECTION p_skipidx ON t_est (SELECT a, b, v ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1);
    EXPLAIN WHATIF SELECT sum(v) FROM t_est WHERE a = 5000 SETTINGS ${PIN};
" | grep -E '^With|^\s+reason:' | awk '{$1=$1; print}'

echo "--- projections disabled by the query ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'

echo "--- use_primary_key = 0 turns the projection's own pruning off, as in the optimizer ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, use_primary_key = 0;
" | grep -E '^\s+reason:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_real WHERE b = 42 SETTINGS ${PIN}, use_primary_key = 0" \
    | grep -oE 'ReadFromMergeTree \(p_b\)' || echo "real: read from the base table"

echo "--- empirical = 0 scans nothing, and reports no marks it does not have ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF empirical = 0 SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN};
" | sed -n '/^With/,$p' | grep -E '^\s+(status|source|empirical_status):|marks:' | awk '{$1=$1; print}'

echo "--- a forced projection must not fail the statement before the candidate is seen ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, force_optimize_projection = 1;
" 2>&1 | grep -E '^\s+(status|verdict):|Code:' | awk '{$1=$1; print}'

# a projection sort key has no direction, so the synthetic part is sorted the only way one can be
echo "--- a query that reads no parts gets no verdict, the optimizer ignores projections then ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_pruned;
    CREATE TABLE t_pruned (d Date, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    INSERT INTO t_pruned SELECT toDate('2020-01-01'), number, number % 100 FROM numbers(1000);
    CREATE HYPOTHETICAL PROJECTION p_all ON t_pruned (SELECT d, a, b ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_pruned WHERE d = '2030-05-05' AND b = 42 SETTINGS ${PIN};
    DROP TABLE t_pruned;
" | grep -E '^\s+(status|reason|verdict):' | awk '{$1=$1; print}'

echo "--- the preferred-projection setting only narrows existing projections, so it is ignored ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_other';
" 2>&1 | grep -E '^\s+(status|verdict):|Code:' | awk '{$1=$1; print}'

echo "--- a descending projection key does not parse ---"
$CLICKHOUSE_CLIENT -q "CREATE HYPOTHETICAL PROJECTION p_desc ON t_est (SELECT a, b, v ORDER BY b DESC);" 2>&1 | grep -m1 -oE 'SYNTAX_ERROR'

echo "--- the scan honours max_rows_to_read ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_est WHERE b = 42 SETTINGS ${PIN}, max_rows_to_read = 100, read_overflow_mode = 'break';
" | grep -E '^\s+(status|source|empirical_status|empirical_reason):' | awk '{$1=$1; print}'

# the scan reads the projection columns, so SELECT is checked at estimate time
echo "--- estimating needs SELECT on the projection columns ---"
user="u_estimate_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user}; CREATE USER ${user} NOT IDENTIFIED;
    GRANT ALTER ADD PROJECTION ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -m1 -oE 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(v) ON ${CLICKHOUSE_DATABASE}.t_est TO ${user};"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE HYPOTHETICAL PROJECTION p_priv ON ${CLICKHOUSE_DATABASE}.t_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_est WHERE b = 42 SETTINGS ${PIN};
" 2>&1 | grep -E '^\s+status:' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real; DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;"
