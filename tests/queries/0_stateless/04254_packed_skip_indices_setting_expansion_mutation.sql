-- Regression: raising packed_skip_index_max_bytes from a value where only the small minmax
-- fits to one where the bloom_filter also fits, then mutating the bloom_filter's column, must:
--   1. Not hardlink the source's skp_idx.packed into the new part. The writer in the new
--      part is about to open skp_idx.packed to add the freshly-packed bloom_filter; a shared
--      inode would be truncated, both corrupting the source and overwriting the minmax bytes.
--   2. Preserve m_v's archive bytes into the new part (its column v isn't in the mutation
--      pipeline, so it can't be recomputed; only carrying over works).
--
-- Setup: source has m_v inside skp_idx.packed (small enough at threshold = 200) and bf_w as a
-- per-file substream (over threshold). Raise the threshold so bf_w also fits, then ALTER
-- UPDATE w. The new part must keep m_v gating granules AND have bf_w packed too.

DROP TABLE IF EXISTS t_expand_mutate;
CREATE TABLE t_expand_mutate
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1,
    INDEX bf_w w TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 200,
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_expand_mutate SELECT number, number * 2, number * 3 FROM numbers(2000);

-- Source layout: m_v packed (small), bf_w per-file (larger than threshold). Assert both are
-- materialized via secondary_indices_compressed_bytes > 0 (works for either layout).
SELECT 'before_expand_indices_materialized', secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_expand_mutate' AND active;

ALTER TABLE t_expand_mutate MODIFY SETTING packed_skip_index_max_bytes = 4194304;
ALTER TABLE t_expand_mutate UPDATE w = w + 1 WHERE id < 100 SETTINGS mutations_sync = 2;

-- Both indices must remain usable: m_v carried over from source archive, bf_w freshly packed.
-- The Skip block's "kept / total" ratio must show kept < total (the index actually filters).
-- Random merge-tree settings move the absolute granule count around, so we only assert that
-- a non-empty Skip block exists with kept < total.
SELECT 'after_mutate_count', count() FROM t_expand_mutate;
SELECT 'm_v_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_expand_mutate WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
SELECT 'bf_w_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_expand_mutate WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_expand_mutate SETTINGS check_query_single_value_result = 1;

DROP TABLE t_expand_mutate;
