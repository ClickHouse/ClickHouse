#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on wide parts whose JSON / Dynamic substreams are DISJOINT across source parts. The merged part's dynamic
# structure is chosen from all source columns (ColumnObject / ColumnDynamic chooseDynamicStructureForMerge), so
# it can have more substreams than any single source part. The output-side estimate must count the union of the
# source parts' substreams (countOutputStreams), not the maximum over parts - otherwise it undercounts here.
# Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything down to a single part and must not error while estimating
# the memory of a merge whose result part has more dynamic substreams than any input part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    SET enable_json_type = 1;
    SET enable_dynamic_type = 1;

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_disjoint (k UInt64, j JSON, d Dynamic)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_disjoint;
    -- Each part contributes a disjoint set of JSON paths and a distinct Dynamic type, so the merged part has
    -- the union of them all - strictly more dynamic substreams than any single source part.
    INSERT INTO t_merge_mem_disjoint SELECT number, ('{\"a0\": ' || toString(number) || ', \"a1\": ' || toString(number) || ', \"a2\": ' || toString(number) || '}')::JSON, number::Dynamic FROM numbers(1000);
    INSERT INTO t_merge_mem_disjoint SELECT number, ('{\"b0\": ' || toString(number) || ', \"b1\": ' || toString(number) || ', \"b2\": ' || toString(number) || '}')::JSON, toString(number)::Dynamic FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_disjoint SELECT number, ('{\"c0\": [' || toString(number) || '], \"c1\": ' || toString(number) || ', \"c2\": \"x\"}')::JSON, (number / 2)::Dynamic FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_disjoint;

    -- Must merge to a single part or throw, never no-op silently, and must not error while estimating memory.
    OPTIMIZE TABLE t_merge_mem_disjoint FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_disjoint;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_disjoint' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
