#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# prefer_optimize_projection would let the baseline read p_wide over far more marks than the parent
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_prefer;
    CREATE TABLE t_prefer (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100, index_granularity_bytes = 0;
    ALTER TABLE t_prefer ADD PROJECTION p_wide (SELECT a, b ORDER BY b);
    INSERT INTO t_prefer SELECT number, cityHash64(number) % 1000 FROM numbers(100000);
"

PIN="optimize_use_projections = 1, optimize_trivial_count_query = 0"

echo "--- query setting ---"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL INDEX hi ON t_prefer (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_prefer WHERE a < 5000 AND b < 900 SETTINGS ${PIN}, prefer_optimize_projection = 1;
" | grep -E '^\s+(marks|status):' | head -2 | awk '{$1=$1; print}'

echo "--- session setting ---"
$CLICKHOUSE_CLIENT -q "
    SET prefer_optimize_projection = 1;
    CREATE HYPOTHETICAL INDEX hi ON t_prefer (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT count() FROM t_prefer WHERE a < 5000 AND b < 900 SETTINGS ${PIN};
" | grep -E '^\s+(marks|status):' | head -2 | awk '{$1=$1; print}'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_prefer"
