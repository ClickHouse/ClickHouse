-- Tags: no-fasttest

-- Row policies on the source table must be enforced by `vectorSearch`: the
-- policy filter is merged into the bitmap prefilter, so policy-hidden rows
-- are never candidates — even when the query has no WHERE clause.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_rp;

CREATE TABLE tab_rp(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_rp VALUES (0, [0.0, 0.0]), (1, [1.0, 0.0]), (2, [2.0, 0.0]), (3, [3.0, 0.0]), (4, [4.0, 0.0]);

DROP ROW POLICY IF EXISTS policy_04330 ON tab_rp;
CREATE ROW POLICY policy_04330 ON tab_rp FOR SELECT USING id % 2 = 0 TO CURRENT_USER;

SELECT '-- the row policy is applied without a WHERE clause';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

SELECT '-- the row policy combines with the WHERE prefilter';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp, idx, [0.0, 0.0], 5) WHERE id > 0 ORDER BY _score ASC, id;

DROP ROW POLICY policy_04330 ON tab_rp;

SELECT '-- without the policy all rows are candidates again';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

DROP TABLE tab_rp;
