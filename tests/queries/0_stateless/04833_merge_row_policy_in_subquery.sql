DROP TABLE IF EXISTS mrp_mt;
DROP TABLE IF EXISTS mrp_pk;
DROP TABLE IF EXISTS mrp_log;
DROP TABLE IF EXISTS mrp_a;
DROP TABLE IF EXISTS mrp_b;

CREATE TABLE mrp_mt (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO mrp_mt VALUES (5, 10), (6, 20);
CREATE ROW POLICY 04833_mt ON mrp_mt FOR SELECT USING value IN (SELECT 10) TO ALL;

SELECT 'final';
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') FINAL ORDER BY id;

SELECT 'plain, prewhere disabled';
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') ORDER BY id
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

SELECT 'global in';
CREATE ROW POLICY 04833_mt_g ON mrp_mt FOR SELECT USING value GLOBAL IN (SELECT 10) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') FINAL ORDER BY id;
DROP ROW POLICY 04833_mt_g ON mrp_mt;

SELECT 'not in';
CREATE ROW POLICY 04833_mt_n ON mrp_mt FOR SELECT USING value NOT IN (SELECT 20) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') FINAL ORDER BY id;
DROP ROW POLICY 04833_mt_n ON mrp_mt;

SELECT 'granules still pruned';
-- index_granularity is pinned so the exact count survives the runner's randomization.
CREATE TABLE mrp_pk (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO mrp_pk SELECT number, number * 10 FROM numbers(40);
CREATE ROW POLICY 04833_pk ON mrp_pk FOR SELECT USING id IN (SELECT 5) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_pk$') ORDER BY id;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM merge(currentDatabase(), '^mrp_pk$')) WHERE explain ILIKE '%Granules: 1/10%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM merge(currentDatabase(), '^mrp_pk$') SETTINGS use_index_for_in_with_subqueries = 0) WHERE explain ILIKE '%Granules: 10/10%';
DROP ROW POLICY 04833_pk ON mrp_pk;

SELECT 'no unconverted delayed step';
SELECT count() FROM (EXPLAIN SELECT * FROM merge(currentDatabase(), '^mrp_mt$') FINAL) WHERE explain ILIKE '%DelayedCreatingSets%';

DROP ROW POLICY 04833_mt ON mrp_mt;

SELECT 'key column, index for in disabled';
CREATE ROW POLICY 04833_mt_k ON mrp_mt FOR SELECT USING id IN (SELECT 5) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') ORDER BY id
SETTINGS use_index_for_in_with_subqueries = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
DROP ROW POLICY 04833_mt_k ON mrp_mt;

SELECT 'nullable, nullIn';
CREATE TABLE mrp_log (id UInt32, value Nullable(UInt32)) ENGINE = Log;
INSERT INTO mrp_log VALUES (5, 10), (6, NULL);
CREATE ROW POLICY 04833_log ON mrp_log FOR SELECT USING nullIn(value, (SELECT 10)) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_log$') ORDER BY id;
DROP ROW POLICY 04833_log ON mrp_log;

SELECT 'two children, two policies';
CREATE TABLE mrp_a (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mrp_b (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO mrp_a VALUES (1, 10), (2, 20);
INSERT INTO mrp_b VALUES (3, 30), (4, 40);
CREATE ROW POLICY 04833_a ON mrp_a FOR SELECT USING value IN (SELECT 10) TO ALL;
CREATE ROW POLICY 04833_b ON mrp_b FOR SELECT USING value IN (SELECT 30) TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_(a|b)$') FINAL ORDER BY id;
DROP ROW POLICY 04833_a ON mrp_a;
DROP ROW POLICY 04833_b ON mrp_b;

SELECT 'no subquery in policy';
CREATE ROW POLICY 04833_plain ON mrp_mt FOR SELECT USING value > 0 TO ALL;
SELECT * FROM merge(currentDatabase(), '^mrp_mt$') FINAL ORDER BY id;
DROP ROW POLICY 04833_plain ON mrp_mt;

DROP TABLE mrp_mt;
DROP TABLE mrp_pk;
DROP TABLE mrp_log;
DROP TABLE mrp_a;
DROP TABLE mrp_b;
