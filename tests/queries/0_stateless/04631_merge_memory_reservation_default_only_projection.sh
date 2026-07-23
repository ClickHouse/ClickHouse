#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path when the projection reads ONLY a late-added DEFAULT column that no source part
# stores. The rebuild synthesizes that column's values from its default expression for every merged row, so the
# temporary projection part's size has nothing to do with the source parts' other columns. Before the fix a part
# storing no projection-required column was priced at its whole uncompressed size, so the fat unrelated payload
# below inflated the temp-part estimate, could flip a genuinely Compact rebuild to Wide and reserve one writer
# buffer per substream - throttling unrelated merges under merges_mutations_memory_usage_soft_limit. The fix
# contributes zero bytes from such parts and prices the synthesized column with a dedicated term instead:
# rows * value size for a fixed-size type (the UInt64 here), per-stream remnant only for a variable-size one.
# OPTIMIZE reserves memory unconditionally, so this must still succeed under a pathologically small soft limit -
# a coverage test that drives exactly the default-filled-only rebuild sizing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    -- Default min_bytes_for_wide_part (10 MiB): the projection's real data (d - 8 bytes per row) rebuilds into
    -- a Compact temp part; only the buggy whole-part fallback (the fat 'payload' column) could push the format
    -- decision toward Wide.
    CREATE TABLE t_merge_mem_default_only_proj
    (
        k UInt64,
        payload String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_default_only_proj;
    INSERT INTO t_merge_mem_default_only_proj SELECT number, repeat('x', 500) FROM numbers(1000);
    INSERT INTO t_merge_mem_default_only_proj SELECT number, repeat('x', 500) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_default_only_proj SELECT number, repeat('x', 500) FROM numbers(2000, 1000);

    -- Metadata-only: no part stores d, and the projection requires nothing else. The merge below rebuilds it
    -- (materialize_projections_on_merge) from rows whose d is synthesized entirely from the DEFAULT expression.
    ALTER TABLE t_merge_mem_default_only_proj ADD COLUMN d UInt64 DEFAULT k * 2;
    ALTER TABLE t_merge_mem_default_only_proj ADD PROJECTION p_default (SELECT d ORDER BY d);
    SYSTEM START MERGES t_merge_mem_default_only_proj;

    OPTIMIZE TABLE t_merge_mem_default_only_proj FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), sum(d = k * 2) FROM t_merge_mem_default_only_proj;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_default_only_proj' AND active AND partition_id = 'all';
    -- The projection must be materialized by the merge and still answer correctly.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_default_only_proj' AND active;
    SELECT max(d) FROM (SELECT d FROM t_merge_mem_default_only_proj ORDER BY d);
" -- --merges_mutations_memory_usage_soft_limit=1
