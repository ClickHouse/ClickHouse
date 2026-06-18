-- Tests for the eliminateRedundantJoin optimization.
-- When a JOIN's output columns all come from one side and the join preserves
-- that side's row count (LEFT ANY, RIGHT ANY, LEFT/RIGHT ASOF), the join
-- can be eliminated entirely.

SET enable_analyzer = 1;
SET query_plan_eliminate_redundant_join = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
SET query_plan_remove_unused_columns = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_merge_filter_into_join_condition = 0;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id Int32, key String, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id Int32, key String, value String, extra String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id Int32, key String, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (1,'a','v1'), (2,'b','v2'), (3,'c','v3'), (4,'d','v4');
INSERT INTO t2 VALUES (2,'b','V2','e2'), (4,'d','V4','e4');
INSERT INTO t3 VALUES (1,'a','W1'), (3,'c','W3');

-- =============================================================================
-- 1. Basic positive cases (join IS eliminated)
-- =============================================================================

SELECT '-- 1a. LEFT ANY -- canonical case';
-- Expect 0: no Join in plan
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';
-- Correctness: same as SELECT id, value FROM t1
SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id;

SELECT '-- 1b. RIGHT ANY -- mirror';
SELECT count() FROM (EXPLAIN header=0 SELECT t2.id, t2.value FROM t1 RIGHT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';
SELECT t2.id, t2.value FROM t1 RIGHT ANY JOIN t2 ON t1.id = t2.id ORDER BY t2.id;

SELECT '-- 1c. LEFT ANY with join_use_nulls';
SET join_use_nulls = 1;
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';
SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id;
SET join_use_nulls = 0;

SELECT '-- 1d. Multi-column join key';
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id AND t1.key = t2.key) WHERE explain LIKE '%Join%';

SELECT '-- 1e. Complex expression in ON clause';
SELECT count() FROM (EXPLAIN header=0 SELECT t1.value FROM t1 LEFT ANY JOIN t2 ON lower(t1.key) = lower(t2.key)) WHERE explain LIKE '%Join%';

SELECT '-- 1f. Join where right side is a heavy subquery';
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN (SELECT id, sum(length(value)) AS total FROM t2 GROUP BY id) sub ON t1.id = sub.id) WHERE explain LIKE '%Join%';

SELECT '-- 1g. Output includes only the join key from preserved side';
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 1h. Self-join';
SELECT count() FROM (EXPLAIN header=0 SELECT a.id, a.value FROM t1 a LEFT ANY JOIN t1 b ON a.id = b.id) WHERE explain LIKE '%Join%';

SELECT '-- 1i. Only constants in output';
SELECT count() FROM (EXPLAIN header=0 SELECT 1 FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';
-- count() should equal t1 row count
SELECT count() FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id;

-- =============================================================================
-- 2. Cascading elimination (multiple joins)
-- =============================================================================

SELECT '-- 2a. Two LEFT ANY joins, both eliminated';
-- Expect 0: no Join in plan
SELECT count() FROM (EXPLAIN header=0
    SELECT t1.id, t1.value
    FROM t1
    LEFT ANY JOIN t2 ON t1.id = t2.id
    LEFT ANY JOIN t3 ON t1.id = t3.id
) WHERE explain LIKE '%Join%';
SELECT t1.id, t1.value
FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id LEFT ANY JOIN t3 ON t1.id = t3.id
ORDER BY t1.id;

SELECT '-- 2b. Partial elimination -- one join preserved';
-- t2 join preserved (extra column is used), t3 join eliminated
SELECT count() > 0 FROM (EXPLAIN header=0
    SELECT t1.id, t2.extra
    FROM t1
    LEFT ANY JOIN t2 ON t1.id = t2.id
    LEFT ANY JOIN t3 ON t1.id = t3.id
) WHERE explain LIKE '%Join%';

SELECT '-- 2c. Chain where INNER blocks cascading';
-- INNER to t2 is preserved (it filters rows)
SELECT count() > 0 FROM (EXPLAIN header=0
    SELECT t1.id
    FROM t1
    INNER JOIN t2 ON t1.id = t2.id
    LEFT ANY JOIN t3 ON t1.id = t3.id
) WHERE explain LIKE '%Join%';

-- =============================================================================
-- 3. Negative cases (join must NOT be eliminated)
-- =============================================================================

SELECT '-- 3a. Right-side column in SELECT';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id, t2.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 3b. Right-side column in ORDER BY';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id ORDER BY t2.value) WHERE explain LIKE '%Join%';

SELECT '-- 3c. Right-side column in GROUP BY';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id, count() FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id GROUP BY t1.id, t2.value) WHERE explain LIKE '%Join%';

SELECT '-- 3d. LEFT ALL -- row multiplication';
DROP TABLE IF EXISTS t_one;
DROP TABLE IF EXISTS t_multi;
CREATE TABLE t_one (id Int32, v String) ENGINE = Memory;
CREATE TABLE t_multi (id Int32, w String) ENGINE = Memory;
INSERT INTO t_one VALUES (1, 'a'), (2, 'b');
INSERT INTO t_multi VALUES (1, 'X'), (1, 'Y'), (1, 'Z');
-- Join must be preserved
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t_one.id FROM t_one LEFT ALL JOIN t_multi ON t_one.id = t_multi.id) WHERE explain LIKE '%Join%';
-- Prove the join multiplies rows: 4 rows, not 2
SELECT t_one.id FROM t_one LEFT ALL JOIN t_multi ON t_one.id = t_multi.id ORDER BY t_one.id;
DROP TABLE t_one;
DROP TABLE t_multi;

SELECT '-- 3e. INNER ANY -- filters non-matching rows';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 INNER ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 3f. LEFT SEMI -- filters';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 3g. LEFT ANTI -- filters';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 3h. CROSS JOIN';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 CROSS JOIN t2) WHERE explain LIKE '%Join%';

SELECT '-- 3i. FULL OUTER JOIN';
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 FULL ALL JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

-- =============================================================================
-- 4. Edge cases
-- =============================================================================

SELECT '-- 4a. Empty right table';
DROP TABLE IF EXISTS t_empty;
CREATE TABLE t_empty (id Int32, value String) ENGINE = Memory;
SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t_empty ON t1.id = t_empty.id ORDER BY t1.id;
DROP TABLE t_empty;

SELECT '-- 4b. Nullable join key on preserved side';
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (id Nullable(Int32), value String) ENGINE = Memory;
INSERT INTO t_nullable VALUES (1, 'a'), (NULL, 'b'), (3, 'c');
SELECT t_nullable.id, t_nullable.value FROM t_nullable LEFT ANY JOIN t2 ON t_nullable.id = t2.id ORDER BY t_nullable.value;
DROP TABLE t_nullable;

SELECT '-- 4c. Column pruning exposes elimination opportunity';
SELECT count() FROM (EXPLAIN header=0
    SELECT id, x FROM (
        SELECT t1.id, t1.value AS x, t2.value AS y, t2.extra AS z
        FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id
    )
) WHERE explain LIKE '%Join%';

-- =============================================================================
-- 5. Real-world BI / view scenarios
-- =============================================================================

SELECT '-- 5a. View with optional enrichment';
DROP VIEW IF EXISTS enriched_view;
CREATE VIEW enriched_view AS
SELECT t1.id AS id, t1.key AS key, t1.value AS value, t2.extra AS label
FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id;

-- BI tool populating a filter dropdown -- join should be eliminated:
SELECT count() FROM (EXPLAIN header=0 SELECT DISTINCT key FROM enriched_view) WHERE explain LIKE '%Join%';
SELECT DISTINCT key FROM enriched_view ORDER BY key;

-- Query that needs the join:
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT id, label FROM enriched_view) WHERE explain LIKE '%Join%';
DROP VIEW enriched_view;

SELECT '-- 5b. Star schema view';
DROP VIEW IF EXISTS star_view;
DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS dim1;
DROP TABLE IF EXISTS dim2;
DROP TABLE IF EXISTS dim3;

CREATE TABLE facts (ts DateTime, metric Float64, d1_id Int32, d2_id Int32, d3_id Int32)
    ENGINE = MergeTree ORDER BY ts;
CREATE TABLE dim1 (id Int32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dim2 (id Int32, name String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dim3 (id Int32, name String) ENGINE = MergeTree ORDER BY id;

INSERT INTO facts VALUES
    ('2024-01-01', 1.0, 1, 1, 1), ('2024-01-02', 2.0, 2, 1, 2),
    ('2024-01-03', 3.0, 1, 2, 1), ('2024-01-04', 4.0, 2, 2, 2);
INSERT INTO dim1 VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO dim2 VALUES (1, 'red'), (2, 'blue');
INSERT INTO dim3 VALUES (1, 'circle'), (2, 'square');

CREATE VIEW star_view AS
SELECT
    f.ts, f.metric,
    d1.name AS dim1_name,
    d2.name AS dim2_name,
    d3.name AS dim3_name
FROM facts f
LEFT ANY JOIN dim1 d1 ON f.d1_id = d1.id
LEFT ANY JOIN dim2 d2 ON f.d2_id = d2.id
LEFT ANY JOIN dim3 d3 ON f.d3_id = d3.id;

-- Only wants facts -- all three joins eliminated (0 joins):
SELECT count() FROM (EXPLAIN header=0 SELECT ts, metric FROM star_view) WHERE explain LIKE '%Join%';

-- Only wants one dimension -- two joins eliminated (1 join):
SELECT count() FROM (EXPLAIN header=0 SELECT ts, metric, dim1_name FROM star_view) WHERE explain LIKE '%Join%';

-- Wants all dimensions -- no elimination (3 joins):
SELECT count() FROM (EXPLAIN header=0 SELECT * FROM star_view) WHERE explain LIKE '%Join%';

DROP VIEW star_view;

-- =============================================================================
-- 6. Setting toggle
-- =============================================================================

SELECT '-- 6a. Setting disabled preserves join';
SET query_plan_eliminate_redundant_join = 0;
SELECT count() > 0 FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

SELECT '-- 6b. Setting enabled eliminates join';
SET query_plan_eliminate_redundant_join = 1;
SELECT count() FROM (EXPLAIN header=0 SELECT t1.id FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id) WHERE explain LIKE '%Join%';

-- =============================================================================
-- 7. Result correctness verification (optimization off vs on must match)
-- =============================================================================

SELECT '-- 7a. LEFT ANY with partial matches';
SET query_plan_eliminate_redundant_join = 0;
SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id;
SET query_plan_eliminate_redundant_join = 1;
SELECT t1.id, t1.value FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id;

SELECT '-- 7b. Cascading elimination correctness';
SET query_plan_eliminate_redundant_join = 0;
SELECT t1.id, t1.value
FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id LEFT ANY JOIN t3 ON t1.id = t3.id
ORDER BY t1.id;
SET query_plan_eliminate_redundant_join = 1;
SELECT t1.id, t1.value
FROM t1 LEFT ANY JOIN t2 ON t1.id = t2.id LEFT ANY JOIN t3 ON t1.id = t3.id
ORDER BY t1.id;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS dim1;
DROP TABLE IF EXISTS dim2;
DROP TABLE IF EXISTS dim3;
