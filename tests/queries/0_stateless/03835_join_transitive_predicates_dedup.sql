-- Tests for predicate deduplication and multiple independent equivalence classes.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;

DROP TABLE IF EXISTS da;
DROP TABLE IF EXISTS db;
DROP TABLE IF EXISTS dc;

CREATE TABLE da (x UInt32, y UInt32, val String) ENGINE = MergeTree PRIMARY KEY (x) SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE db (x UInt32, y UInt32, val String) ENGINE = MergeTree PRIMARY KEY (x) SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc (x UInt32, y UInt32, val String) ENGINE = MergeTree PRIMARY KEY (x) SETTINGS auto_statistics_types = 'uniq';

INSERT INTO da SELECT number, number * 10, concat('a', toString(number)) FROM numbers(10);
INSERT INTO db SELECT number, number * 10, concat('b', toString(number)) FROM numbers(1000);
INSERT INTO dc SELECT number, number * 10, concat('c', toString(number)) FROM numbers(10);

-- ==========================================================================
-- Case 5: Duplicate predicate within INNER JOIN is removed
-- ==========================================================================

SELECT '-- Case 5: duplicate predicate removed';
SELECT da.x, db.x
FROM da INNER JOIN db ON da.x = db.x AND da.x = db.x
ORDER BY da.x
LIMIT 5;

-- Verify EXPLAIN shows only one clause (not two identical ones)
SELECT '-- Case 5: EXPLAIN shows single clause';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM da INNER JOIN db ON da.x = db.x AND da.x = db.x
) WHERE explain LIKE '%Clauses%';

-- ==========================================================================
-- Case 6: Two independent equivalence classes
-- Class 1: {da.x, db.x, dc.x}
-- Class 2: {da.y, db.y, dc.y}
-- Both should produce synthesized predicates independently.
-- ==========================================================================

SELECT '-- Case 6: two equivalence classes - correctness';
SELECT count()
FROM da, db, dc
WHERE da.x = db.x AND db.x = dc.x
  AND da.y = db.y AND db.y = dc.y;

SELECT count()
FROM da, db, dc
WHERE da.x = db.x AND db.x = dc.x
  AND da.y = db.y AND db.y = dc.y
SETTINGS enable_join_transitive_predicates = 0;

SELECT '-- Case 6: two equivalence classes - values';
SELECT da.val, dc.val
FROM da, db, dc
WHERE da.x = db.x AND db.x = dc.x
  AND da.y = db.y AND db.y = dc.y
ORDER BY da.x LIMIT 5;

SELECT da.val, dc.val
FROM da, db, dc
WHERE da.x = db.x AND db.x = dc.x
  AND da.y = db.y AND db.y = dc.y
ORDER BY da.x LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 6b: EXPLAIN for two equivalence classes -- each join step should
-- have clauses for both key columns.
-- ==========================================================================

SELECT '-- Case 6b: EXPLAIN two classes';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM da, db, dc
    WHERE da.x = db.x AND db.x = dc.x
      AND da.y = db.y AND db.y = dc.y
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize'
) WHERE explain LIKE '%Clauses%' OR explain LIKE '%ReadFromMergeTree%' OR (explain LIKE '%Type: %' AND explain NOT LIKE '%ReadType%');

DROP TABLE da;
DROP TABLE db;
DROP TABLE dc;
