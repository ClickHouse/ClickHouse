#!/usr/bin/env bash
# Tags: no-object-storage
# an estimate reads the part and must not write to it: the builder it takes the synthetic part from
# reclaims a `.tmp_proj` directory left by an interrupted materialization, which a read may not do

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_writes;
    CREATE TABLE t_writes (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 100, min_bytes_for_wide_part = 0;
    SYSTEM STOP MERGES t_writes;
    INSERT INTO t_writes SELECT number, number % 100 FROM numbers(1000);
"

part=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_writes' AND active")
mkdir -p "${part}p_candidate.tmp_proj"
echo kept > "${part}p_candidate.tmp_proj/leftover"

$CLICKHOUSE_CLIENT -q "
    CREATE HYPOTHETICAL PROJECTION p_candidate ON t_writes (SELECT a, b ORDER BY b);
    EXPLAIN WHATIF SELECT count() FROM t_writes WHERE b = 42
        SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0, optimize_use_projections = 1;
" | grep -oE 'source:\s+[a-z_]+' | awk '{print $2}'

echo "leftover kept: $([ -f "${part}p_candidate.tmp_proj/leftover" ] && echo 1 || echo 0)"
rm -rf "${part}p_candidate.tmp_proj"
