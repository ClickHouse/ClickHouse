-- Regression: shrinking packed_skip_index_max_bytes to an empty (or non-overlapping) value, then
-- mutating one of the indexed columns, must preserve the untouched in-archive indices.
--
-- Before the fix, the writer did not create skip_indices_packed_writer (current setting packs
-- nothing) so the preload of preserved entries silently no-op'd and the new part lost the
-- untouched index's data. CHECK TABLE still passed because the inherited skp_idx.packed
-- checksum was removed, but the index became unmaterialized (Granules: 2/2 in EXPLAIN).

DROP TABLE IF EXISTS t_shrink_mutate;
CREATE TABLE t_shrink_mutate
(
    id UInt64,
    v UInt64,
    w UInt64,
    INDEX m_v v TYPE minmax GRANULARITY 1,
    INDEX m_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
         packed_skip_index_max_bytes = 4194304,
         auto_statistics_types = '',
         index_granularity = 1024;

INSERT INTO t_shrink_mutate SELECT number, number * 2, number * 3 FROM numbers(2000);

ALTER TABLE t_shrink_mutate MODIFY SETTING packed_skip_index_max_bytes = 0;
ALTER TABLE t_shrink_mutate UPDATE v = v + 1 WHERE id < 100 SETTINGS mutations_sync = 2;

-- m_v is freshly recomputed under the new (per-file) layout; m_w was untouched and must
-- have been carried over from the source's skp_idx.packed into the new part's archive.
SELECT 'count_after', count() FROM t_shrink_mutate;
SELECT 'm_v_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_shrink_mutate WHERE v = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
SELECT 'm_w_filtered', countIf(
    splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[1]::UInt64
    < splitByChar('/', trim(replaceAll(explain, 'Granules:', '')))[2]::UInt64)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_shrink_mutate WHERE w = 250) AS s
WHERE explain LIKE '%Granules:%' AND explain NOT LIKE '%PrimaryKey%' SETTINGS allow_experimental_analyzer = 1;
CHECK TABLE t_shrink_mutate SETTINGS check_query_single_value_result = 1;

DROP TABLE t_shrink_mutate;
