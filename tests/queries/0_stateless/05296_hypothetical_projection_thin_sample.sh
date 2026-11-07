#!/usr/bin/env bash
# fewer sampled rows than the part has granules: each sampled row stands for ten granules here, and the
# twin table with the projection materialized is the ground truth the sampled span has to contain

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# the implicit minmax indices of add_minmax_index_for_numeric_columns change the plans and estimates this test pins

PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, prefer_optimize_projection = 0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_thin; DROP TABLE IF EXISTS t_thin_real;
    CREATE TABLE t_thin (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
    CREATE TABLE t_thin_real AS t_thin;
    ALTER TABLE t_thin_real ADD PROJECTION p_b (SELECT * ORDER BY b);
    INSERT INTO t_thin SELECT number, cityHash64(number) % 1000 FROM numbers(600);
    INSERT INTO t_thin_real SELECT number, cityHash64(number) % 1000 FROM numbers(600);
"

# prints how many granules were sampled and whether the real read falls inside the span the estimate reports
for where in "b < 500" "b < 100"; do
    out=$($CLICKHOUSE_CLIENT -q "
        CREATE HYPOTHETICAL PROJECTION p_b ON t_thin (SELECT * ORDER BY b);
        EXPLAIN WHATIF projection_scan_budget_rows = 60 SELECT count() FROM t_thin WHERE ${where} SETTINGS ${PIN};
        SELECT '--- real ---';
        EXPLAIN indexes = 1 SELECT count() FROM t_thin_real WHERE ${where} SETTINGS ${PIN}, prefer_optimize_projection = 1;
    ")
    est=$(sed -n '1,/^--- real ---$/p' <<< "$out" | grep -E '^\s+marks:' | tail -1 | awk '{print $2}')
    span=$(grep -oE 'marks_span:\s+[0-9]+ to [0-9]+' <<< "$out" | grep -oE '[0-9]+ to [0-9]+')
    low=$(awk '{print $1}' <<< "${span:-$est to $est}")
    high=$(awk '{print $3}' <<< "${span:-$est to $est}")
    real=$(sed -n '/^--- real ---$/,$p' <<< "$out" | grep -oE 'Granules: [0-9]+$' | head -1 | awk '{print $2}')
    sampled=$(grep -oE 'sampled_marks:\s+[0-9]+ / [0-9]+' <<< "$out" | awk '{print $2 " / " $4}')
    echo "${where}: sampled ${sampled}, real inside the span $(( real >= low && real <= high ? 1 : 0 ))"
done
