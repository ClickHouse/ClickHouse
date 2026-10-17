#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on wide parts that contain JSON / Dynamic columns. Their real on-disk substreams are data-dependent and are
# not visible in the default serialization, so the estimate must read the actual substream layout from the part
# (columns_substreams.txt) instead of undercounting them. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, so it must
# still merge everything down to a single part and must not error while estimating the memory of a merge that
# has dynamic substreams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    SET enable_json_type = 1;
    SET enable_dynamic_type = 1;

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_dynamic (k UInt64, j JSON, d Dynamic)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_dynamic;
    INSERT INTO t_merge_mem_dynamic SELECT number, ('{\"a\": ' || toString(number) || ', \"s' || toString(number % 8) || '\": \"x\"}')::JSON, number::Dynamic FROM numbers(1000);
    INSERT INTO t_merge_mem_dynamic SELECT number, ('{\"b\": ' || toString(number) || ', \"s' || toString(number % 8) || '\": ' || toString(number) || '}')::JSON, toString(number)::Dynamic FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_dynamic SELECT number, ('{\"a\": ' || toString(number) || ', \"c\": [' || toString(number) || ']}')::JSON, (number / 2)::Dynamic FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_dynamic;

    -- Must merge to a single part or throw, never no-op silently, and must not error while estimating memory.
    OPTIMIZE TABLE t_merge_mem_dynamic FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_dynamic;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dynamic' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
