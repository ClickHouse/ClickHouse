#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# sampled estimates on parts whose layout the sample cannot see whole

PIN="optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1, prefer_optimize_projection = 0"

# the wide rows sit in one granule the sample may skip, so their width can only be assumed
echo "--- variable-width rows on a sampled estimate ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_scan_w;
    CREATE TABLE t_scan_w (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 4096, min_bytes_for_wide_part = 0;
    INSERT INTO t_scan_w SELECT number, number % 1000, if(number BETWEEN 15000 AND 15099, repeat('x', 1000), '') FROM numbers(30000);
    CREATE HYPOTHETICAL PROJECTION p_w ON t_scan_w (SELECT a, b, s ORDER BY b);
    EXPLAIN WHATIF max_rows_to_scan = 3000 SELECT a, s FROM t_scan_w WHERE b < 300 SETTINGS ${PIN};
" | grep -oE 'verdict: +[a-z ]+|not known to have the same width' | awk '{$1=$1; print}'

# granules of different sizes in a compact part: the reader resumes at each sampled granule
echo "--- a sampled compact part with uneven granules ---"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_scan_c;
    CREATE TABLE t_scan_c (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 1000, index_granularity_bytes = 4096, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
            merge_max_block_size = 8192;
    INSERT INTO t_scan_c SELECT number, number % 1000, '' FROM numbers(10000);
    INSERT INTO t_scan_c SELECT number + 10000, number % 1000, repeat('x', 200) FROM numbers(10000);
    OPTIMIZE TABLE t_scan_c FINAL;
    CREATE HYPOTHETICAL PROJECTION p_c ON t_scan_c (SELECT a, b, s ORDER BY b);
    EXPLAIN WHATIF max_rows_to_scan = 1000 SELECT a FROM t_scan_c WHERE b < 10 SETTINGS ${PIN};
" 2>&1 | grep -oE 'empirical_status: +[a-z]+|Code: [0-9]+' | awk '{$1=$1; print}'
