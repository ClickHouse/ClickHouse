-- Regression: expanding packed_skip_index_types to cover a new index type, then mutating a
-- column of that type, must:
--   1. Not hardlink the source's skp_idx.packed into the new part (the writer is also about
--      to write into skp_idx.packed; a shared inode would be truncated, corrupting source).
--   2. Preserve the surviving in-archive indices (their columns aren't in the mutation
--      pipeline, so they can't be recomputed; their bytes must be carried over from source).
--
-- Setup: source has a packed minmax(v) and a per-file bloom_filter(w). Expand the setting to
-- cover bloom_filter, then ALTER UPDATE w. The new part must keep m_v working AND have a
-- newly-packed bf_w.

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
         packed_skip_index_types = 'minmax',
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_expand_mutate SELECT number, number * 2, number * 3 FROM numbers(2000);

-- Source layout: only minmax is packed; bloom_filter is per-file.
SELECT 'before_expand_packed_only_minmax', name, files FROM system.parts
WHERE database = currentDatabase() AND table = 't_expand_mutate' AND active;

ALTER TABLE t_expand_mutate MODIFY SETTING packed_skip_index_types = 'minmax, bloom_filter';
ALTER TABLE t_expand_mutate UPDATE w = w + 1 WHERE id < 100 SETTINGS mutations_sync = 2;

-- Both indices must remain usable: m_v carried over from source archive, bf_w freshly packed.
SELECT 'after_mutate_count', count() FROM t_expand_mutate;
SELECT 'm_v_granules', explain
FROM (EXPLAIN indexes = 1 SELECT * FROM t_expand_mutate WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' SETTINGS allow_experimental_analyzer = 1;
SELECT 'bf_w_granules', explain
FROM (EXPLAIN indexes = 1 SELECT * FROM t_expand_mutate WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_expand_mutate SETTINGS check_query_single_value_result = 1;

DROP TABLE t_expand_mutate;
