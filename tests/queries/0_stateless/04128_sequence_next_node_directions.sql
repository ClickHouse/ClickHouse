-- Exercise AggregateFunctionSequenceNextNode direction × base matrix,
-- partitioned aggregation via GROUP BY (merge path), and error paths.
--
-- Argument convention (after the seq_direction / seq_base parameters):
--   sequenceNextNode(...)(time, event, base_cond, chain_0, chain_1, ...)
-- Where:
--   base_cond -- per-row gate that decides which rows can be selected as base.
--   chain_i   -- positional matcher checked at base ± i (depending on direction).
-- The result is the value of `event` at base + events_size for forward direction
-- (or base - events_size for backward), if all chain matchers succeed.
--
-- The test data is a single sequence ['X','A','B','C','A','B','D'] with strictly
-- monotonic timestamps. By choosing different base / chain combinations we steer
-- each (direction × base) cell to a distinct, non-NULL result so that a regression
-- in any branch of getBaseIndex / getNextNodeIndex would surface as a diff.

SET allow_experimental_funnel_functions = 1;

DROP TABLE IF EXISTS seq_events;
CREATE TABLE seq_events (ts DateTime, e String) ENGINE = Memory;
INSERT INTO seq_events
SELECT toDateTime('2020-01-01 00:00:0'||toString(number)) AS ts,
       arrayElement(['X','A','B','C','A','B','D'], number+1) AS e
FROM numbers(7);

-- forward + head: base = pos 0 (X). chain[0]=X at 0, chain[1]=A at 1 -> event at pos 2 = B.
SELECT '--- forward + head ---';
SELECT sequenceNextNode('forward', 'head')(ts, e, e IN ('X','A','B'), e='X', e='A') FROM seq_events;

-- forward + first_match: first row with e IN (X,A,B) AND e='A' = pos 1 (A).
-- chain[1]=B at 2 -> event at pos 3 = C.
SELECT '--- forward + first_match ---';
SELECT sequenceNextNode('forward', 'first_match')(ts, e, e IN ('X','A','B'), e='A', e='B') FROM seq_events;

-- forward + last_match: last row with e IN (X,A,B) AND e='A' = pos 4 (A).
-- chain[1]=B at 5 -> event at pos 6 = D.
SELECT '--- forward + last_match ---';
SELECT sequenceNextNode('forward', 'last_match')(ts, e, e IN ('X','A','B'), e='A', e='B') FROM seq_events;

-- backward + tail: base = pos 6 (D). chain[0]=D at 6, chain[1]=B at 5 -> event at pos 4 = A.
SELECT '--- backward + tail ---';
SELECT sequenceNextNode('backward', 'tail')(ts, e, e IN ('A','B','D'), e='D', e='B') FROM seq_events;

-- backward + first_match: first row with e IN (A,B) AND e='B' = pos 2 (B).
-- chain[1]=A at 1 -> event at pos 0 = X.
SELECT '--- backward + first_match ---';
SELECT sequenceNextNode('backward', 'first_match')(ts, e, e IN ('A','B'), e='B', e='A') FROM seq_events;

-- backward + last_match: last row with e IN (A,B) AND e='B' = pos 5 (B).
-- chain[1]=A at 4 -> event at pos 3 = C.
SELECT '--- backward + last_match ---';
SELECT sequenceNextNode('backward', 'last_match')(ts, e, e IN ('A','B'), e='B', e='A') FROM seq_events;

-- Partitioned merge: same chain across two partitions, exercising the per-state
-- merge path. p1 events sorted by time = [A, C, B]; p2 = [X, A, C].
-- chain[0]=e='A', chain[1]=e='C' -> p1 returns event at pos 2 = B; p2's match falls
-- off the end of the sequence and returns NULL. The non-NULL branch ensures a
-- regression to "always NULL" is caught.
SELECT '--- partitioned with GROUP BY (merge path) ---';
SELECT p, sequenceNextNode('forward', 'first_match')(ts, e, e IN ('A','B','C','X'), e='A', e='C')
FROM (SELECT arrayElement(['p1','p2'], (number % 2) + 1) AS p,
             toDateTime('2020-01-01 00:00:0' || toString(number)) AS ts,
             arrayElement(['A','X','C','A','B','C'], number+1) AS e
      FROM numbers(6))
GROUP BY p
ORDER BY p;

-- Sequence shorter than chain: returns NULL.
SELECT '--- too few events (size <= events_size): NULL ---';
SELECT sequenceNextNode('forward', 'first_match')(ts, e, true, e='A', e='B', e='C')
FROM (SELECT toDateTime('2020-01-01 00:00:00') AS ts, 'A' AS e);

-- Empty input: NULL.
SELECT '--- empty input ---';
SELECT sequenceNextNode('forward', 'first_match')(ts, e, true, e='A', e='B')
FROM (SELECT toDateTime('2020-01-01') AS ts, 'A' AS e WHERE 0);

-- Long chain (8 matchers) on a perfectly aligned sequence A..I -> returns I.
SELECT '--- long chain forward ---';
SELECT sequenceNextNode('forward', 'first_match')(
    ts, e, e IN ('A','B','C','D','E','F','G','H','I'),
    e='A', e='B', e='C', e='D', e='E', e='F', e='G', e='H')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS ts,
             arrayElement(['A','B','C','D','E','F','G','H','I'], number+1) AS e
      FROM numbers(9));

-- -Distinct combinator: smoke test (single-matcher path; events_size = 0).
SELECT '--- -Distinct combinator (events_size=0 returns base event) ---';
SELECT sequenceNextNodeDistinct('forward','head')(ts, e, e='A')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS ts,
             arrayElement(['A','B','A','B','A','B'], number+1) AS e
      FROM numbers(6));

-- Error paths.
SELECT '--- error: invalid direction ---';
SELECT sequenceNextNode('bad', 'head')(ts, e, e='A')
FROM (SELECT toDateTime('2020-01-01') AS ts, 'A' AS e); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: invalid base ---';
SELECT sequenceNextNode('forward', 'bad')(ts, e, e='A')
FROM (SELECT toDateTime('2020-01-01') AS ts, 'A' AS e); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: forward + tail is rejected ---';
SELECT sequenceNextNode('forward', 'tail')(ts, e, e='A')
FROM (SELECT toDateTime('2020-01-01') AS ts, 'A' AS e); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: backward + head is rejected ---';
SELECT sequenceNextNode('backward', 'head')(ts, e, e='A')
FROM (SELECT toDateTime('2020-01-01') AS ts, 'A' AS e); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: event column must be String ---';
SELECT sequenceNextNode('forward', 'head')(ts, e, e=1)
FROM (SELECT toDateTime('2020-01-01') AS ts, 1 AS e); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE seq_events;
