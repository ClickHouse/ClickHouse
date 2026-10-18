#!/usr/bin/env bash
# Tags: no-fasttest, no-random-merge-tree-settings
# The `use_skip_indexes_if_final_exact_mode` recovery pass must cost about the same for selected granules
# scattered over many parts with disjoint key ranges as for the same number of granules in one part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_exact_mode_disjoint_parts"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_exact_mode_disjoint_parts (key UInt64, ver UInt64, v UInt8, INDEX idx_v v TYPE minmax GRANULARITY 1)
    ENGINE = ReplacingMergeTree(ver) ORDER BY key
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi'"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_exact_mode_disjoint_parts"

insert_settings="max_insert_threads = 1, max_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0"
# 200 parts with disjoint key ranges, every other granule has `v = 1`.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_exact_mode_disjoint_parts SELECT number, 1, number % 2 = 0 FROM numbers(16000)
    SETTINGS $insert_settings, max_block_size = 80, max_insert_block_size = 80"
# One part above all of them, every other granule has `v = 2`.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_exact_mode_disjoint_parts SELECT 1000000 + number, 1, if(number % 2 = 0, 2, 0) FROM numbers(16000)
    SETTINGS $insert_settings, max_block_size = 16000, max_insert_block_size = 16000"

$CLICKHOUSE_CLIENT -q "
    SELECT rows, count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_exact_mode_disjoint_parts' AND active
    GROUP BY rows ORDER BY rows"

settings="use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1, use_query_condition_cache = 0,
    enable_parallel_replicas = 0, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0, max_threads = 1"

# Both predicates select 8000 disjoint key intervals and the recovery pass adds back 8000 granules for each.
for v in 1 2; do
    $CLICKHOUSE_CLIENT -q "SELECT parts, marks FROM (EXPLAIN ESTIMATE SELECT key FROM t_exact_mode_disjoint_parts FINAL WHERE v = $v SETTINGS $settings)"
done

explain_time()
{
    $CLICKHOUSE_CLIENT --time -q "EXPLAIN ESTIMATE SELECT key FROM t_exact_mode_disjoint_parts FINAL WHERE v = $1 SETTINGS $settings" 2>&1 >/dev/null | tail -n 1
}

times=()
for _ in 1 2 3; do
    times+=("$(explain_time 1)" "$(explain_time 2)")
done

# Best of three runs: the scattered granules may take at most three times as long as the granules in one part.
echo "${times[*]}" | awk '{
    if (NF != 6) { print "bad timing: " $0; exit }
    for (i = 1; i <= NF; ++i) if ($i !~ /^[0-9.]+(e-?[0-9]+)?$/) { print "bad timing: " $0; exit }
    scattered = $1; one_part = $2
    for (i = 3; i < NF; i += 2) { if ($i < scattered) scattered = $i; if ($(i + 1) < one_part) one_part = $(i + 1) }
    if (scattered <= 3 * one_part + 0.1) print "OK"; else printf "scattered: %.3f s, one part: %.3f s\n", scattered, one_part
}'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_exact_mode_disjoint_parts"
