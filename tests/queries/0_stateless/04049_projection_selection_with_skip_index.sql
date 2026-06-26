-- Tags: no-parallel-replicas

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100783: with
-- `use_skip_indexes_on_data_read = 1` and a projection present on the table, the base-table skip
-- index used to be silently ignored at data-read time, scanning the whole table. The skip index
-- filtering must be performed (and accounted for) before deciding between the base table and a
-- projection.

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET optimize_projection_skip_index_ratio = 0.5;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id1 UInt32,
    id2 String,
    v1 UInt32,
    INDEX v1_index v1 TYPE minmax,
    PROJECTION proj1 (SELECT * ORDER BY id2)
)
ENGINE = MergeTree
ORDER BY id1
SETTINGS
    index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1;

INSERT INTO tab SELECT number + 1, concat('k', toString(number + 1)), number + 1 FROM numbers(10000);

SELECT COUNT(*) FROM tab WHERE v1 <= 500;

SELECT COUNT(*) FROM tab WHERE id2 > 'k3';

-- The base table is chosen: the minmax index on `v1` filters `v1 <= 500` down to far fewer marks
-- (8) than the `id2`-ordered projection would read for `id2 > 'k3'` (123). The `EXPLAIN` confirms
-- the read source is the main table (`default.tab`), not the projection.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT COUNT(*) FROM tab WHERE id2 > 'k3' AND v1 <= 500
) WHERE explain ILIKE '%ReadFromMergeTree%';
-- The skip index must actually be applied to the base read (issue #100783): reading more than
-- 10 granules would mean the index was ignored and the whole table scanned.
SELECT COUNT(*) FROM tab WHERE id2 > 'k3' AND v1 <= 500 SETTINGS max_rows_to_read = 640; -- 10 granules

SELECT COUNT(*) FROM tab WHERE id2 >= 'k9990';

-- The projection is chosen: here the minmax index on `v1` is not selective (`v1 >= 5000` keeps ~79
-- marks), while the `id2`-ordered projection reads a single mark for `id2 >= 'k9990'`. The `EXPLAIN`
-- confirms the read source is the projection (`proj1`).
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT COUNT(*) FROM tab WHERE id2 >= 'k9990' AND v1 >= 5000
) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT COUNT(*) FROM tab WHERE id2 >= 'k9990' AND v1 >= 5000 SETTINGS max_rows_to_read = 128; -- 2 granules

DROP TABLE tab;
