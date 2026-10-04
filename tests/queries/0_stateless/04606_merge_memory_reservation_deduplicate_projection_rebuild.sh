#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# of a deduplicating merge on parts that predate a projection. `OPTIMIZE ... DEDUPLICATE` makes the merge
# row-reducing (`merge_may_reduce_rows`), so it REBUILDS every projection from the merged rows even though no
# source part has the projection materialized - a path the estimate must price as a rebuild rather than as a
# dropped projection (the table sets deduplicate_merge_projection_mode = 'rebuild', which OPTIMIZE DEDUPLICATE
# requires on a table with projections). The projection selects a `JSON` column, so the rebuilt temporary projection parts carry
# dynamic substreams as well. Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit
# OPTIMIZE reserves unconditionally, so it must still deduplicate down to a single part with the projection
# materialized, and must not error while estimating the memory of the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_dedup_rebuild (k UInt64, json JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, deduplicate_merge_projection_mode = 'rebuild';

    -- Two identical parts: FINAL DEDUPLICATE must collapse the duplicate rows.
    SYSTEM STOP MERGES t_merge_mem_dedup_rebuild;
    INSERT INTO t_merge_mem_dedup_rebuild SELECT number, toJSONString(map('a', number)) FROM numbers(1000);
    INSERT INTO t_merge_mem_dedup_rebuild SELECT number, toJSONString(map('a', number)) FROM numbers(1000);

    -- Added after the parts are written, so no source part has it materialized: the deduplicating merge
    -- takes the rebuild path for it regardless of presence.
    ALTER TABLE t_merge_mem_dedup_rebuild ADD PROJECTION p_json (SELECT k, json ORDER BY k);
    SYSTEM START MERGES t_merge_mem_dedup_rebuild;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_dedup_rebuild FINAL DEDUPLICATE SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_dedup_rebuild;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dedup_rebuild' AND active;
    -- The projection must have been rebuilt into the merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_dedup_rebuild' AND active
        ORDER BY name;
    -- And the data must be correct after the merge.
    SELECT sum(json.a::UInt64) FROM t_merge_mem_dedup_rebuild;
" -- --merges_mutations_memory_usage_soft_limit=1
