#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: uses the server-global failpoint whatif_projection_scan_cut_short
# a projection scan that stops early, as a time limit in `break` mode stops it, must not give an estimate

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_whatif_break;
    CREATE TABLE t_whatif_break (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_whatif_break SELECT number, cityHash64(number) % 1000 FROM numbers(20000);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT whatif_projection_scan_cut_short"
$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_b ON t_whatif_break (SELECT a, b ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_whatif_break WHERE b < 100
    SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 1, prefer_optimize_projection = 0;
" | grep -E '^\s+empirical_(status|reason):' | awk '{$1=$1; print}'
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT whatif_projection_scan_cut_short"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_whatif_break"
