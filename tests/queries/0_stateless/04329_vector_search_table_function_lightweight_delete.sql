-- Tags: no-fasttest

-- Rows hidden by lightweight deletes are excluded through the bitmap
-- prefilter. When the query has no WHERE clause, an implicit `_row_exists`
-- prefilter is pushed into the bitmap subquery (a masked read), so the
-- scorer never emits deleted rows that the lazy reader would refuse to read.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_lwd;

CREATE TABLE tab_lwd(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_lwd VALUES (0, [0.0, 0.0]), (1, [1.0, 0.0]), (2, [2.0, 0.0]), (3, [3.0, 0.0]), (4, [4.0, 0.0]);

DELETE FROM tab_lwd WHERE id = 1;

SELECT '-- no WHERE: the deleted row is excluded by the implicit prefilter';
SELECT id FROM vectorSearch(currentDatabase(), tab_lwd, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

SELECT '-- no WHERE: the implicit prefilter gates the scorer pipeline';
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab_lwd, idx, [0.0, 0.0], 5))
WHERE explain LIKE '%DelayedPorts%';

SELECT '-- with WHERE: the deleted row is excluded by the user prefilter';
SELECT id FROM vectorSearch(currentDatabase(), tab_lwd, idx, [0.0, 0.0], 5) WHERE id >= 0 ORDER BY _score ASC, id;

OPTIMIZE TABLE tab_lwd FINAL;

SELECT '-- after merges remove the deleted rows, no implicit prefilter is built';
SELECT id FROM vectorSearch(currentDatabase(), tab_lwd, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab_lwd, idx, [0.0, 0.0], 5))
WHERE explain LIKE '%DelayedPorts%';

DROP TABLE tab_lwd;
