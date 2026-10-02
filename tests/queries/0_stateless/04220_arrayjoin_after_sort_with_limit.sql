-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/82279
-- The `tryExecuteFunctionsAfterSorting` optimization in
-- `liftUpFunctions.cpp` used to lift expressions above the `SortingStep`
-- even when those expressions contained `arrayJoin`. With a `LIMIT`
-- pushed down into the `SortingStep`, the input rows were truncated
-- *before* `arrayJoin` expansion, silently dropping rows that should
-- have been produced.

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS l;

CREATE TABLE m(id UInt32) ENGINE = Memory AS SELECT number FROM numbers(10);
CREATE TABLE l(id UInt32, a Array(UInt16)) ENGINE = Memory AS SELECT 4, [1, 2, 3, 4, 5];

-- The original repro from the issue.
SELECT '-- LIMIT 30 (matches all expansions)';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 30);

SELECT '-- LIMIT 3 (used to return 0 rows)';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 3);

SELECT '-- LIMIT 1';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 1);

SELECT '-- LIMIT 2';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 2);

SELECT '-- LIMIT 4';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 4);

SELECT '-- LIMIT 100';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 100);

-- The bug also reproduces without `JOIN` on a plain table where the
-- first sorted rows have empty arrays. With `arrayJoin` lifted above
-- the `SortingStep` the `LIMIT` would keep those empty-array rows and
-- the expansion would produce zero output.
DROP TABLE IF EXISTS t;
CREATE TABLE t(id UInt32, a Array(UInt16)) ENGINE = Memory;
INSERT INTO t VALUES (1, []), (2, []), (3, []), (4, [10, 20, 30, 40, 50]), (5, []);

SELECT '-- No JOIN, LIMIT 3';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM t ORDER BY id LIMIT 3);
DROP TABLE t;

-- Verify the optimization is still applied for non-`arrayJoin` expressions.
-- The expression `id + 1` must still be lifted above `SortingStep`. The
-- description of the lifted step differs between the new and the old
-- analyzer, so check only for presence of the `[lifted up part]` marker.
SELECT '-- Plain expression: optimization still applies';
SELECT count() > 0 FROM (EXPLAIN actions = 0 SELECT id, id + 1 AS s FROM m ORDER BY id LIMIT 3) WHERE explain LIKE '%lifted up part%';

-- Same bug on the old analyzer path.
SELECT '-- Old analyzer, LIMIT 3';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM m LEFT JOIN l ON l.id = m.id ORDER BY id LIMIT 3) SETTINGS allow_experimental_analyzer = 0;

-- When `arrayJoin` is the sort key, it stays below `SortingStep` and
-- the optimization is not applied — verify this still works.
SELECT '-- ORDER BY arrayJoin output, LIMIT 3';
SELECT count() FROM (SELECT id, arrayJoin(a) AS x FROM l ORDER BY x LIMIT 3);

DROP TABLE m;
DROP TABLE l;
