-- Tags: no-fasttest

-- `vectorSearch` exposes every virtual column of the underlying MergeTree
-- source table (`_part`, `_part_index`, `_part_offset`, `_partition_id`,
-- `_partition_value`, `_block_number`, ...) in addition to its own `_score`.
-- The MergeTree virtuals are materialized by the lazy reader, exactly as in a
-- normal SELECT from the source table. They are virtual columns, so they do
-- not appear in `SELECT *` or `DESCRIBE` (only the source columns and `_score`
-- do), but they are queryable explicitly and usable in WHERE.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
PARTITION BY (id % 2)
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- Three inserts -> three parts spread over two partitions.
INSERT INTO tab VALUES (0, [1.0, 0.0]), (2, [1.1, 0.0]);
INSERT INTO tab VALUES (1, [0.0, 1.0]), (3, [0.0, 1.1]);
INSERT INTO tab VALUES (4, [2.0, 0.0]), (6, [2.1, 0.0]);

SELECT '-- Source columns + _score appear in SELECT *, MergeTree virtuals do not';
SELECT * FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2) ORDER BY id;

SELECT '-- DESCRIBE lists the source columns, _score, and the _distance alias (no MergeTree virtuals)';
DESCRIBE vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2);

SELECT '-- The MergeTree virtual columns are queryable explicitly';
SELECT id, _part_index, _part_offset, _partition_id, _partition_value, _block_number, _block_offset, _part_data_version, _row_exists
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6)
ORDER BY id;

-- `_disk_name` is exposed too, but its value depends on the storage policy
-- (e.g. `default` locally vs. an object-storage disk under the s3 CI config),
-- so only confirm that it is populated rather than print the literal name.
SELECT '-- _disk_name is exposed (value is storage-policy dependent)';
SELECT DISTINCT _disk_name != '' FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6);

SELECT '-- WHERE on a partition virtual column';
SELECT id, _partition_id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6) WHERE _partition_id = '0' ORDER BY id;

SELECT '-- WHERE on the _part_offset locator virtual (post-filtered, not pushed into the prefilter)';
SELECT id, _part_offset FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6) WHERE _part_offset = 0 ORDER BY id;

SELECT '-- WHERE on the _part_index locator virtual';
SELECT count() FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6) WHERE _part_index = 0;

SELECT '-- _block_number distinguishes the three source parts';
SELECT _block_number, count() FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6) GROUP BY _block_number ORDER BY _block_number;

DROP TABLE tab;
