#!/usr/bin/env bash
# the layout a merge leaves is not recorded in a part, so the estimate spans the layouts the writer
# could have produced and the real one has to fall inside that span

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# count() must reach the read step and the real projection must be allowed to win
PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, optimize_read_in_order = 1"

# the merge's block size sets the granule size, and a part records neither the blocks it was handed
# nor the cuts the merge made, so the estimate spans the layouts and the real one has to fall inside
echo "--- a merged part whose granules follow the merge's block size ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_est_m; DROP TABLE IF EXISTS t_real_m;
    CREATE TABLE t_est_m (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 1000, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0,
                 min_rows_for_wide_part = 0, use_const_adaptive_granularity = 0, merge_max_block_size = 100;
    CREATE TABLE t_real_m AS t_est_m;
    ALTER TABLE t_real_m ADD PROJECTION p_m (SELECT a, b, v ORDER BY b);
    SYSTEM STOP MERGES t_est_m; SYSTEM STOP MERGES t_real_m;
    INSERT INTO t_est_m SELECT number, number % 100, number FROM numbers(500);
    INSERT INTO t_real_m SELECT number, number % 100, number FROM numbers(500);
    INSERT INTO t_est_m SELECT number, number % 100, number FROM numbers(500, 500);
    INSERT INTO t_real_m SELECT number, number % 100, number FROM numbers(500, 500);
    SYSTEM START MERGES t_est_m; SYSTEM START MERGES t_real_m;
    OPTIMIZE TABLE t_est_m FINAL; OPTIMIZE TABLE t_real_m FINAL;
"
out=$($CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_m ON t_est_m (SELECT a, b, v ORDER BY b);
    EXPLAIN WHATIF SELECT sum(v) FROM t_est_m WHERE b >= 0 SETTINGS ${PIN};
    -- a full scan reads every granule the projection has, so the real layout is its own mark count
    SELECT 'real granules:', marks - 1 FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_real_m' AND active;
")
est=$(grep -E '^\s+marks:' <<< "$out" | tail -1 | awk '{print $2}')
# no span means every layout came out the same, so the estimate is the whole of it
span=$(grep -oE 'marks_span:\s+[0-9]+ to [0-9]+' <<< "$out" | grep -oE '[0-9]+ to [0-9]+')
low=$(awk '{print $1}' <<< "${span:-$est to $est}")
high=$(awk '{print $3}' <<< "${span:-$est to $est}")
real=$(awk -F'\t' '/^real granules:/ {print $2}' <<< "$out")
# the two block sizes lay this part out differently whatever the merge really did, so the spread is reported
echo "marks_span reported: $(grep -c 'marks_span:' <<< "$out")"
echo "the real layout is inside the estimated span: $(( real >= low && real <= high ? 1 : 0 ))"
