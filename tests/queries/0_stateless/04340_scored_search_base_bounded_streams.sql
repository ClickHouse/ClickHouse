-- Tags: no-fasttest

-- The scorer pipeline must not create one source per part: a bounded number
-- of worker sources (min(num_streams, parts)) pull parts from a shared queue,
-- so tables with many parts do not blow up the processor count or the merge
-- fan-in.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_many_parts;

CREATE TABLE tab_many_parts(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab_many_parts;

INSERT INTO tab_many_parts VALUES (0, [0.0, 0.0]);
INSERT INTO tab_many_parts VALUES (1, [1.0, 0.0]);
INSERT INTO tab_many_parts VALUES (2, [2.0, 0.0]);
INSERT INTO tab_many_parts VALUES (3, [3.0, 0.0]);
INSERT INTO tab_many_parts VALUES (4, [4.0, 0.0]);
INSERT INTO tab_many_parts VALUES (5, [5.0, 0.0]);
INSERT INTO tab_many_parts VALUES (6, [6.0, 0.0]);
INSERT INTO tab_many_parts VALUES (7, [7.0, 0.0]);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_many_parts' AND active;

SELECT '-- the number of scorer sources is bounded by max_threads, not the part count';
SELECT extract(explain, 'ScorerSource[^"]*') FROM (EXPLAIN PIPELINE graph = 1 SELECT id FROM vectorSearch(currentDatabase(), tab_many_parts, idx, [0.0, 0.0], 8) SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1) WHERE explain LIKE '%ScorerSource%';

SELECT '-- and by the part count when max_threads is larger';
SELECT extract(explain, 'ScorerSource[^"]*') FROM (EXPLAIN PIPELINE graph = 1 SELECT id FROM vectorSearch(currentDatabase(), tab_many_parts, idx, [0.0, 0.0], 8) SETTINGS max_threads = 32, max_streams_to_max_threads_ratio = 1) WHERE explain LIKE '%ScorerSource%';

SELECT '-- results are identical regardless of the worker count';
SELECT id FROM vectorSearch(currentDatabase(), tab_many_parts, idx, [0.0, 0.0], 8) ORDER BY _score, id SETTINGS max_threads = 1;
SELECT id FROM vectorSearch(currentDatabase(), tab_many_parts, idx, [0.0, 0.0], 8) ORDER BY _score, id SETTINGS max_threads = 4;

SELECT '-- the WHERE prefilter combines with the shared part queue';
SELECT id FROM vectorSearch(currentDatabase(), tab_many_parts, idx, [0.0, 0.0], 8) WHERE id >= 3 ORDER BY _score, id SETTINGS max_threads = 2;

SYSTEM START MERGES tab_many_parts;
DROP TABLE tab_many_parts;
