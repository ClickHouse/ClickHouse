-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/82279
-- (LIMIT BY variant uncovered by the original PR #104558).
--
-- `pushLimitByIntoSort` would attach a per-stream `LimitByTransform` to a
-- `SortingStep` that has `arrayJoin` lifted above it. The per-stream hint
-- truncates input rows BEFORE `arrayJoin` expansion, silently dropping rows
-- that should have produced output.

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32, pos UInt32, a Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();

-- Stop merges so the four parts inserted below stay separate. The bad plan
-- needs the per-stream `LimitByTransform` to run before the lifted `arrayJoin`,
-- and that transform only fires when the read pipeline has multiple streams
-- (`pipeline.getNumStreams() > 1`), which requires multiple parts. If a
-- background merge collapsed the parts into one before the `SELECT` runs, an
-- unfixed build would read a single stream, never attach the per-stream limit,
-- and the test would become a false negative.
SYSTEM STOP MERGES t;

-- Multiple parts so the read pipeline has multiple streams (per-stream
-- `LimitByTransform` only fires when `pipeline.getNumStreams() > 1`).
INSERT INTO t VALUES (1, 1, []), (1, 2, [1]);
INSERT INTO t VALUES (2, 1, [10]), (2, 2, []);
INSERT INTO t VALUES (3, 1, []), (3, 2, [30]);
INSERT INTO t VALUES (4, 1, [40]), (4, 2, []);

-- Expected: 4 rows. Without the fix, ids whose empty-array row sorts first
-- (id=1 and id=3) lose the non-empty row to the per-stream limit.
SELECT '-- LIMIT 1 BY id';
SELECT id, arrayJoin(a) AS v FROM t ORDER BY id, pos LIMIT 1 BY id SETTINGS max_threads = 2;

SELECT '-- Old analyzer';
SELECT id, arrayJoin(a) AS v FROM t ORDER BY id, pos LIMIT 1 BY id SETTINGS max_threads = 2, allow_experimental_analyzer = 0;

-- The `LIMIT BY` pushdown is still applied when the expression above the
-- sort does not contain `arrayJoin`. Verify the optimization still fires by
-- looking for the "Per-stream LIMIT BY" marker on the `SortingStep`.
SELECT '-- Plain expression: pushdown still applied';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, pos + 1 AS s FROM t ORDER BY id, pos LIMIT 1 BY id) WHERE explain LIKE '%Per-stream LIMIT BY%';

DROP TABLE t;
