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
    INSERT INTO t_served SELECT number, number % 100, number FROM numbers(1000);
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
    INSERT INTO t_est_s SELECT (number % 100, number), number FROM numbers(1000);
    INSERT INTO t_real_s SELECT (number % 100, number), number FROM numbers(1000);
"
compare p_s "(SELECT t, v ORDER BY t.x)" "SELECT t, v FROM TABLE WHERE t.x = 42" t_est_s t_real_s
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_s; DROP TABLE IF EXISTS t_real_s;"

# a projection index also stores the parent offset, so a byte-driven granularity has to count it
echo "--- a byte-driven projection index counts the parent offset it stores ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_i; DROP TABLE IF EXISTS t_real_i;
    CREATE TABLE t_est_i (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_real_i AS t_est_i;
    ALTER TABLE t_real_i ADD PROJECTION p_i INDEX b TYPE basic;
    INSERT INTO t_est_i SELECT number, intDiv(number, 100), number FROM numbers(5000);
    INSERT INTO t_real_i SELECT number, intDiv(number, 100), number FROM numbers(5000);
"
compare p_i "INDEX b TYPE basic" "SELECT count() FROM TABLE WHERE b = 7" t_est_i t_real_i
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_i; DROP TABLE IF EXISTS t_real_i;"

# a merged part whose row width varies along the projection key: the writer sizes a granule per block
# it stores, so one average over the part would be several times out
echo "--- a merged part whose rows differ in width along the projection key ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_w; DROP TABLE IF EXISTS t_real_w;
    CREATE TABLE t_est_w (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 8192, index_granularity_bytes = '128Ki', min_bytes_for_wide_part = 0,
                 merge_max_block_size = 8192, merge_max_block_size_bytes = '10Mi',
                 use_const_adaptive_granularity = 0;
    CREATE TABLE t_real_w AS t_est_w;
    ALTER TABLE t_real_w ADD PROJECTION p_w (SELECT a, b, s ORDER BY b);
    -- narrow rows carry the low part of the key, wide rows the high part
    INSERT INTO t_est_w SELECT number, number, repeat('x', 20) FROM numbers(4000);
    INSERT INTO t_est_w SELECT number + 4000, number + 4000, repeat('y', 2000) FROM numbers(400);
    INSERT INTO t_real_w SELECT number, number, repeat('x', 20) FROM numbers(4000);
    INSERT INTO t_real_w SELECT number + 4000, number + 4000, repeat('y', 2000) FROM numbers(400);
    OPTIMIZE TABLE t_est_w FINAL; OPTIMIZE TABLE t_real_w FINAL;
"
# the granule layout of such a part depends on the blocks the writer was fed, which is not recorded,
# so the estimate is held to the documented margin of a granule instead of to the exact count
for w in "b >= 4000" "b < 2000"; do
    est=$($CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION p_w ON t_est_w (SELECT a, b, s ORDER BY b);
        EXPLAIN WHATIF SELECT a, s FROM t_est_w WHERE ${w} SETTINGS ${PIN};
    " | grep -E '^\s+marks:' | tail -1 | awk '{print $2}')
    real=$($CLICKHOUSE_CLIENT -q "
        EXPLAIN indexes = 1 SELECT a, s FROM t_real_w WHERE ${w} SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_w';
    " | grep -oE 'Granules: [0-9]+' | head -1 | awk '{print $2}')
    # a whole-part average would be several times out on the wide half of the key
    echo "${w}: within a granule of the real count: $(( est >= real - 1 && est <= real + 1 ? 1 : 0 ))"
done
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_w; DROP TABLE IF EXISTS t_real_w;"

# a lightweight delete leaves the dead rows in the part, and a projection without a WHERE keeps them:
# the rebuild ANDs `_row_exists` into the projection's WHERE, and there is none here to AND it into
echo "--- a materialized lightweight delete ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_d; DROP TABLE IF EXISTS t_real_d;
    CREATE TABLE t_est_d (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
                 lightweight_mutation_projection_mode = 'rebuild';
    CREATE TABLE t_real_d AS t_est_d;
    ALTER TABLE t_real_d ADD PROJECTION p_d (SELECT a, b, v ORDER BY b);
    INSERT INTO t_est_d SELECT number, number % 100, number FROM numbers(1000);
    INSERT INTO t_real_d SELECT number, number % 100, number FROM numbers(1000);
    DELETE FROM t_est_d WHERE a % 2 = 0 SETTINGS lightweight_deletes_sync = 2;
    DELETE FROM t_real_d WHERE a % 2 = 0 SETTINGS lightweight_deletes_sync = 2;
    SELECT 'rows the rebuilt projection kept:', sum(rows) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_real_d' AND active;
"
compare p_d "(SELECT a, b, v ORDER BY b)" "SELECT a, v FROM TABLE WHERE b < 30" t_est_d t_real_d
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_d; DROP TABLE IF EXISTS t_real_d;"

# a projection that stores less per row gets bigger granules, so a full scan of it beats the base read
# even when the predicate cannot prune its key and there is no ORDER BY to help
echo "--- a full projection scan that is cheaper than the base read ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_n; DROP TABLE IF EXISTS t_real_n;
    CREATE TABLE t_est_n (a UInt64, b UInt64, pad String) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 8192, index_granularity_bytes = '16Ki', min_bytes_for_wide_part = 0;
    CREATE TABLE t_real_n AS t_est_n;
    ALTER TABLE t_real_n ADD PROJECTION p_n (SELECT a, b ORDER BY b);
    INSERT INTO t_est_n SELECT number, number % 1000, repeat('x', 500) FROM numbers(5000);
    INSERT INTO t_real_n SELECT number, number % 1000, repeat('x', 500) FROM numbers(5000);
"
compare p_n "(SELECT a, b ORDER BY b)" "SELECT sum(b) FROM TABLE WHERE a % 7 = 3" t_est_n t_real_n
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est_n; DROP TABLE IF EXISTS t_real_n;"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_est; DROP TABLE IF EXISTS t_real; DROP TABLE IF EXISTS t_est_g; DROP TABLE IF EXISTS t_real_g;"
