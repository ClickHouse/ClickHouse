-- Tags: no-fasttest

-- A row policy with `IN (subquery)` registers its set in a prepared-sets
-- registry private to the policy's expression analyzer, so no interpreter-level
-- step builds it. `vectorSearch` must build the set inside the bitmap-prefilter
-- subquery plan; without that the query fails with a `Not-ready Set` exception
-- whenever the predicate is not moved to PREWHERE.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_rp_in;
DROP TABLE IF EXISTS allowed_tenants;

CREATE TABLE tab_rp_in(id Int32, tenant UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_rp_in VALUES (0, 1, [0.0, 0.0]), (1, 2, [1.0, 0.0]), (2, 1, [2.0, 0.0]), (3, 2, [3.0, 0.0]), (4, 3, [4.0, 0.0]);

CREATE TABLE allowed_tenants(tenant UInt64) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO allowed_tenants VALUES (1), (3);

DROP ROW POLICY IF EXISTS policy_04338 ON tab_rp_in;
CREATE ROW POLICY policy_04338 ON tab_rp_in FOR SELECT USING tenant IN (SELECT tenant FROM allowed_tenants) TO CURRENT_USER;

SELECT '-- the policy set is built when the predicate moves to PREWHERE';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp_in, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id;

SELECT '-- and when it stays in the explicit filter step';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp_in, idx, [0.0, 0.0], 5) ORDER BY _score ASC, id
SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- the policy combines with an outer WHERE that also uses IN (subquery)';
SELECT id FROM vectorSearch(currentDatabase(), tab_rp_in, idx, [0.0, 0.0], 5) WHERE id IN (SELECT 2 UNION ALL SELECT 3) ORDER BY _score ASC, id
SETTINGS optimize_move_to_prewhere = 0;

DROP ROW POLICY policy_04338 ON tab_rp_in;
DROP TABLE tab_rp_in;
DROP TABLE allowed_tenants;
