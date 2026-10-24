#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# when a column with a DEFAULT is stored by SOME source parts and missing from others - one tiny part predating
# ALTER ... ADD COLUMN plus larger parts written after it. Only the rows of the parts that do NOT store the column
# have its values synthesized by IMergeTreeReader::evaluateMissingDefaults; the rows of the parts that do store it
# are read, so their written bytes are already inside the input-volume bound. Before the fix the whole merge's row
# count was charged for such a column - on both the base output path and the rebuilt-projection path - which
# nearly doubled the writer-side bound on this exact upgrade shape and could reject the merge under
# merges_mutations_memory_usage_soft_limit, the very starvation this estimate exists to avoid. The fix prices only
# the missing rows (countRowsMissingColumn). OPTIMIZE reserves unconditionally, so this must still succeed under a
# pathologically small soft limit - a coverage test that drives exactly the partially-present-default sizing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_partial_default
    (
        k UInt64,
        payload String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_partial_default;
    -- One small part written BEFORE the ALTER: it does not store d at all.
    INSERT INTO t_merge_mem_partial_default SELECT number, repeat('x', 500) FROM numbers(100);

    -- Metadata-only ALTER: the parts written before it keep no d at all.
    ALTER TABLE t_merge_mem_partial_default ADD COLUMN d UInt64 DEFAULT k * 2;

    -- Two much larger parts written AFTER the ALTER: they physically store d, so its bytes for their rows are
    -- already accounted as read input and must not be charged a second time as synthesized volume.
    INSERT INTO t_merge_mem_partial_default SELECT number, repeat('x', 500), number * 2 FROM numbers(100, 1450);
    INSERT INTO t_merge_mem_partial_default SELECT number, repeat('x', 500), number * 2 FROM numbers(1550, 1450);

    -- The projection is added last, so no part has it materialized and every part's projection set matches
    -- (OPTIMIZE refuses to merge parts with different projection sets); the merge below rebuilds it from the
    -- merged rows, driving the rebuilt-projection sizing for the same partially-present column.
    ALTER TABLE t_merge_mem_partial_default ADD PROJECTION p_partial (SELECT d ORDER BY d);
    SYSTEM START MERGES t_merge_mem_partial_default;

    OPTIMIZE TABLE t_merge_mem_partial_default FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), sum(d = k * 2) FROM t_merge_mem_partial_default;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_partial_default' AND active AND partition_id = 'all';
    -- The projection must be materialized by the merge and still answer correctly.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_partial_default' AND active;
    SELECT max(d) FROM (SELECT d FROM t_merge_mem_partial_default ORDER BY d);
" -- --merges_mutations_memory_usage_soft_limit=1
