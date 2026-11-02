-- Tags: no-random-merge-tree-settings, no-random-settings

-- The runtime dataflow statistics sampler serializes the columns straight out of the
-- MergeTree reader. A column under an unfinished on-the-fly mutation (e.g. CLEAR COLUMN)
-- is read partially at that point: only the array offsets are read from the part, and the
-- nested column stays empty until `fillMissingColumns` completes it. Serializing such a
-- column produced a `ColumnArray` with inconsistent offsets and threw a logical error.
-- A block with a partially read column must not be serialized; the statistics are marked
-- unsupported for the query instead, because scaling the block's total read bytes with a
-- compression ratio sampled from the remaining columns would poison the cached estimate.

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';
SET enable_analyzer = 1;
SET use_uncompressed_cache = 0;

DROP TABLE IF EXISTS t_clear_arr;

CREATE TABLE t_clear_arr (c0 Int, c1 Array(Tuple(c2 Int)))
ENGINE = MergeTree() ORDER BY tuple()
-- The part must be compact: per-column sizes are unavailable for compact parts, which is
-- what makes the sampler serialize the columns instead of using the on-disk sizes.
SETTINGS apply_mutations_on_fly = 1, min_bytes_for_wide_part = 1e18, auto_statistics_types = '';

SYSTEM STOP MERGES t_clear_arr;

-- A single part, so the one block the sampler picks is always the one with the
-- partially read column.
INSERT INTO t_clear_arr (c0, c1) VALUES (1, [tuple(5)]);
ALTER TABLE t_clear_arr CLEAR COLUMN c1 SETTINGS alter_sync = 0;

SELECT * FROM t_clear_arr ORDER BY c0;

DROP TABLE t_clear_arr;
