-- The subquery ORDER BY decides the order in which sequence functions see events that share a timestamp.
-- One function per query: the sort is kept for the whole aggregation as soon as one function is
-- order-dependent, so a shared query would hide a function that is not.

SET query_plan_remove_redundant_sorting = 1;
SET enable_funnel_functions = 1;

DROP TABLE IF EXISTS ev;
DROP TABLE IF EXISTS nodes;
DROP TABLE IF EXISTS ev2;

-- One block in a fixed order that is neither ascending nor descending in `x`; every event shares t = 0.
CREATE TABLE ev (t UInt32, x UInt8) ENGINE = Memory;
INSERT INTO ev VALUES (0, 2), (0, 4), (0, 6), (0, 0), (0, 1), (0, 3), (0, 5), (0, 7);
CREATE TABLE nodes (t UInt32, page String, x UInt8) ENGINE = Memory;
INSERT INTO nodes VALUES (0, 'A', 2), (0, 'A', 3), (0, 'A', 1), (1, 'B', 4);
-- `ORDER BY x` alone leaves `t` unsorted, so the function sorts the events by `t` and tied ones must keep the `x` order.
CREATE TABLE ev2 (t UInt32, x UInt8) ENGINE = Memory;
INSERT INTO ev2 VALUES (0, 2), (1, 5), (0, 4), (0, 1), (1, 0), (0, 3);

SELECT sequenceMatch('(?1)(?2)(?3)(?4)')(t, x = 1, x = 2, x = 3, x = 4) FROM (SELECT * FROM ev ORDER BY t, x);
SELECT sequenceMatch('(?4)(?3)(?2)(?1)')(t, x = 1, x = 2, x = 3, x = 4) FROM (SELECT * FROM ev ORDER BY t, x DESC);
SELECT sequenceCount('(?1)(?2)')(t, x % 2 = 1, x % 2 = 0) FROM (SELECT * FROM ev ORDER BY t, x);
SELECT sequenceCount('(?1)(?2)')(t, x % 2 = 1, x % 2 = 0) FROM (SELECT * FROM ev ORDER BY t, x DESC);
SELECT sequenceMatchEvents('(?1)(?2)(?3)(?4)')(t, x = 1, x = 2, x = 3, x = 4) FROM (SELECT * FROM ev ORDER BY t, x);
SELECT sequenceMatchEvents('(?4)(?3)(?2)(?1)')(t, x = 1, x = 2, x = 3, x = 4) FROM (SELECT * FROM ev ORDER BY t, x DESC);
SELECT sequenceMatch('(?1)(?2)(?3)(?4)(?5)(?5)')(t, x = 1, x = 2, x = 3, x = 4, x IN (0, 5)) FROM (SELECT * FROM ev2 ORDER BY x);
SELECT sequenceMatch('(?4)(?3)(?2)(?1)(?5)(?5)')(t, x = 1, x = 2, x = 3, x = 4, x IN (0, 5)) FROM (SELECT * FROM ev2 ORDER BY x DESC);
SELECT sequenceNextNode('forward', 'head')(t, page, x = 1, page = 'A', page = 'A', page = 'A') FROM (SELECT * FROM nodes ORDER BY t, x);
SELECT sequenceNextNode('forward', 'head')(t, page, x = 3, page = 'A', page = 'A', page = 'A') FROM (SELECT * FROM nodes ORDER BY t, x DESC);

DROP TABLE ev;
DROP TABLE ev2;
DROP TABLE nodes;
