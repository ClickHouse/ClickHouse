-- Tags: no-fasttest, no-shared-catalog
-- no-shared-catalog: SYSTEM STOP MERGES and on-the-fly mutation timing differ with shared catalog

-- A pending `ALTER DELETE` hides rows without touching any column: it writes no `_row_exists` and
-- contributes nothing to `getAllUpdatedColumns`. The scorer reads the vector index instead of the
-- data, so it would rank rows that a plain `SELECT` from the source table no longer returns.
-- Routing the scorer through the bitmap subquery, which reads through the `MergeTree` readers,
-- keeps the two consistent.

SET allow_experimental_search_topk_table_functions = 1;
SET apply_mutations_on_fly = 1;
SET mutations_sync = 0;

DROP TABLE IF EXISTS tab_alter_delete;

CREATE TABLE tab_alter_delete(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_alter_delete VALUES (0, [0.0, 0.0]), (1, [1.0, 0.0]), (2, [2.0, 0.0]), (3, [3.0, 0.0]);

SYSTEM STOP MERGES tab_alter_delete;

ALTER TABLE tab_alter_delete DELETE WHERE id = 1;

SELECT '-- reference: the plain read of the source table does not return the deleted row';
SELECT id FROM tab_alter_delete ORDER BY id;

SELECT '-- no WHERE: the deleted row is excluded as well';
SELECT id FROM vectorSearch(currentDatabase(), tab_alter_delete, idx, [0.0, 0.0], 4) ORDER BY _score ASC, id;

SELECT '-- no WHERE: the scorer is gated behind the bitmap subquery';
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab_alter_delete, idx, [0.0, 0.0], 4))
WHERE explain LIKE '%DelayedPorts%';

SELECT '-- with WHERE: the deleted row is excluded by the user prefilter';
SELECT id FROM vectorSearch(currentDatabase(), tab_alter_delete, idx, [0.0, 0.0], 4) WHERE id >= 0 ORDER BY _score ASC, id;

SELECT '-- the mutation is not applied with apply_mutations_on_fly = 0, and neither is it here';
SELECT id FROM tab_alter_delete ORDER BY id SETTINGS apply_mutations_on_fly = 0;
SELECT id FROM vectorSearch(currentDatabase(), tab_alter_delete, idx, [0.0, 0.0], 4) ORDER BY _score ASC, id SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES tab_alter_delete;

DROP TABLE tab_alter_delete;
