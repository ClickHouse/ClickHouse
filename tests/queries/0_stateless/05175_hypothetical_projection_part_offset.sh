#!/usr/bin/env bash
# a projection that does not store parent offsets cannot serve an offset predicate, one that stores them is not pruned by its own offsets

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PIN="optimize_use_projections = 1, optimize_use_implicit_projections = 0, optimize_read_in_order = 1, prefer_optimize_projection = 0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_off_est; DROP TABLE IF EXISTS t_off_real;
    CREATE TABLE t_off_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_off_real AS t_off_est;
    ALTER TABLE t_off_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
    ALTER TABLE t_off_real ADD PROJECTION p_ix INDEX b TYPE basic;
    SYSTEM STOP MERGES t_off_est; SYSTEM STOP MERGES t_off_real;
    -- one part, b = a % 100, so sorted by b every granule of the projection holds its own range of b
    INSERT INTO t_off_est SELECT number, number % 100, number FROM numbers(300);
    INSERT INTO t_off_real SELECT number, number % 100, number FROM numbers(300);
"

# the estimate must report the marks the forced real read of the same projection needs
compare()
{
    local projection_name="$1" projection_body="$2" query="$3"
    echo -n "estimated marks: "
    $CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION ${projection_name} ON t_off_est ${projection_body};
        EXPLAIN WHATIF ${query/TABLE/t_off_est} SETTINGS ${PIN};
    " | grep -A 2 "^With ${projection_name} " | grep -oE 'marks: +[0-9]+' | grep -oE '[0-9]+'
    echo -n "real granules:   "
    $CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 ${query/TABLE/t_off_real} SETTINGS ${PIN}, preferred_optimize_projection_name = '${projection_name}', force_optimize_projection = 1" \
        | grep -oE 'Granules: [0-9]+$' | grep -oE '[0-9]+'
}

# a projection that does not store parent offsets cannot serve an offset predicate, so the estimate does not apply
echo "--- a projection that does not store parent offsets cannot serve _part_offset ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_off_est (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT a FROM t_off_est WHERE b >= 34 AND b <= 70 AND _part_offset < 100 SETTINGS ${PIN};
" | grep -A 3 '^With p_b ' | grep -oE 'does not contain column _part_offset'
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT a FROM t_off_real WHERE b >= 34 AND b <= 70 AND _part_offset < 100 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_b', force_optimize_projection = 1" 2>&1 \
    | grep -oE 'PROJECTION_NOT_USED' | head -1

echo "--- the key alone, for comparison ---"
compare p_b "(SELECT a, b, v ORDER BY b)" "SELECT a FROM TABLE WHERE b >= 34 AND b <= 70"

# a projection that stores parent offsets is read with the predicate rewritten to _parent_part_offset,
# so its own offsets never prune, and the plan falls back to the base table
echo "--- a projection storing parent offsets is not pruned by its own offsets ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_ix ON t_off_est INDEX b TYPE basic;
    EXPLAIN WHATIF SELECT _part_offset FROM t_off_est WHERE b >= 34 AND b <= 70 AND _part_offset < 100 SETTINGS ${PIN};
" | grep -A 8 '^With p_ix ' | grep -E '^\s+(marks|verdict):' | awk '{$1=$1; print}'
# the database name is random, so name only what the plan reads
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT _part_offset FROM t_off_real WHERE b >= 34 AND b <= 70 AND _part_offset < 100 SETTINGS ${PIN}, preferred_optimize_projection_name = 'p_ix'" \
    | grep -oE 'ReadFromMergeTree \([^)]*\)' | sed -e 's/.*p_ix.*/the plan reads: p_ix/' -e 's/.*t_off_real.*/the plan reads: the base table/'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_off_est; DROP TABLE t_off_real"
