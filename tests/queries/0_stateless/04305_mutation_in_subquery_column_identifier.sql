-- Regression test for a logical error "Column identifier ... is already registered"
-- in mutations (lightweight DELETE / ALTER UPDATE / ALTER DELETE) whose predicate
-- contains an IN / EXISTS subquery that reads from a default table expression
-- (`system.one` for a FROM-less SELECT) nested in another subquery. The set subquery
-- was planned without unique table aliases, so two table expressions exposing a
-- column with the same name (e.g. `dummy`) produced the same bare column identifier.

DROP TABLE IF EXISTS test_filter;

CREATE TABLE test_filter (a Int32, b Int32, c Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO test_filter SELECT number, number + 1, ((number / 2) + 1) % 2 FROM numbers(15);

-- Original AST-fuzzer reproducer: EXISTS over a FROM-less subquery (reads `system.one`).
-- `exists` is always true, so all rows are deleted.
DELETE FROM test_filter WHERE exists((SELECT DISTINCT * LIMIT 452));
SELECT count() FROM test_filter;

-- IN over a subquery nesting another subquery that reads `system.one`.
INSERT INTO test_filter SELECT number, number + 1, ((number / 2) + 1) % 2 FROM numbers(15);
DELETE FROM test_filter WHERE 1 IN (SELECT 1 FROM (SELECT * FROM system.one));
SELECT count() FROM test_filter;

-- The same shape on the heavy mutation paths.
INSERT INTO test_filter SELECT number, number + 1, ((number / 2) + 1) % 2 FROM numbers(15);
ALTER TABLE test_filter UPDATE b = 0 WHERE 1 IN (SELECT 1 FROM (SELECT * FROM numbers(3))) SETTINGS mutations_sync = 2;
SELECT sum(b) FROM test_filter;

ALTER TABLE test_filter DELETE WHERE exists((SELECT DISTINCT * FROM system.one)) SETTINGS mutations_sync = 2;
SELECT count() FROM test_filter;

-- A set that genuinely filters rows must keep working.
INSERT INTO test_filter SELECT number, number + 1, ((number / 2) + 1) % 2 FROM numbers(15);
DELETE FROM test_filter WHERE c IN (SELECT 0);
SELECT count() FROM test_filter;

DROP TABLE test_filter;
