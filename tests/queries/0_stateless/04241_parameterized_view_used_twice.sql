-- https://github.com/ClickHouse/ClickHouse/issues/105153
-- Two calls of the same parameterized view with different argument values must not
-- be deduplicated by the analyzer when used as IN-subqueries in the same WHERE clause.

DROP TABLE IF EXISTS testing_105153;
DROP VIEW IF EXISTS test_view_105153;

CREATE TABLE testing_105153 (id String) ENGINE = MergeTree() ORDER BY id;
CREATE VIEW test_view_105153 AS SELECT id FROM testing_105153 WHERE id = {id:String};

INSERT INTO testing_105153 VALUES ('a');
INSERT INTO testing_105153 VALUES ('b');

-- Expected: no rows (no value can be both 'a' and 'b').
SELECT id
FROM testing_105153
WHERE (id IN (SELECT id FROM test_view_105153(id = 'a')))
  AND (id IN (SELECT id FROM test_view_105153(id = 'b')))
ORDER BY id;

-- Expected: 'a' and 'b' (the union of both sets).
SELECT id
FROM testing_105153
WHERE (id IN (SELECT id FROM test_view_105153(id = 'a')))
   OR (id IN (SELECT id FROM test_view_105153(id = 'b')))
ORDER BY id;

DROP VIEW test_view_105153;
DROP TABLE testing_105153;
