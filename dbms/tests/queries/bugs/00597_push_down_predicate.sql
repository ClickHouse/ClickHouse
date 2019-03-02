DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;

CREATE TABLE test.test(date Date, id Int8, name String, value Int64) ENGINE = MergeTree(date, (id, date), 8192);
CREATE VIEW test.test_view AS SELECT * FROM test.test;

SET enable_optimize_predicate_expression = 1;
SET enable_debug_queries = 1;

-- Optimize predicate expression with view
-- TODO: simple view is not replaced with subquery inside syntax analyzer
ANALYZE SELECT * FROM test.test_view WHERE id = 1;
ANALYZE SELECT * FROM test.test_view WHERE id = 2;
ANALYZE SELECT id FROM test.test_view WHERE id  = 1;
ANALYZE SELECT s.id FROM test.test_view AS s WHERE s.id = 1;

-- TODO: this query shouldn't work, because the name `toUInt64(sum(id))` is undefined for user
SELECT * FROM (SELECT toUInt64(b), sum(id) AS b FROM test.test) WHERE `toUInt64(sum(id))` = 3;
