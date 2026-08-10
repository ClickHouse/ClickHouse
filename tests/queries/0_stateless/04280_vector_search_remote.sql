-- Tags: no-fasttest

-- Per plan 02 §"Coverage matrix" + §step 7-C: vectorSearch consumed
-- through `remote(...)`. The happy path: a SELECT against
-- `remote('127.0.0.1', db, t)` with a vectorSearch subquery in the
-- WHERE clause routes the table function locally; the remote ships
-- back the matched rows.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]), (4, [0.5, 0.5]);

SELECT '-- Local top-3 (sanity)';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
ORDER BY _score, id;

SELECT '-- remote() ingests vectorSearch ids via IN subquery';
SELECT id
FROM remote('127.0.0.1', currentDatabase(), tab)
WHERE id IN (
    SELECT id
    FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
)
ORDER BY id;

DROP TABLE tab;
