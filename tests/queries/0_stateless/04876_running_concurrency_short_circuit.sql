-- Regression test: `runningConcurrency` must not be lazily executed as a short-circuit argument.
-- Lazy execution filters the argument column down to the rows the mask selects, so the function
-- would never see the rows the mask skipped and would report a concurrency computed over a
-- subset. Every event below overlaps every other, so the value on a masked row differs from the
-- value on the same row of the full set, and the two answers cannot coincide by accident.

DROP TABLE IF EXISTS events_04876;
DROP TABLE IF EXISTS running_04876;

CREATE TABLE events_04876 (i UInt32, s DateTime, e DateTime) ENGINE = Memory;
INSERT INTO events_04876 SELECT number, toDateTime('2020-01-01 00:00:00') + number, toDateTime('2020-01-01 00:00:00') + 1000 FROM numbers(8);

-- Ground truth: the running value is materialized on its own, so no mask can reach it. The `if`
-- is applied afterwards, to a value already computed over all eight rows.
CREATE TABLE running_04876 ENGINE = Memory AS
    SELECT i, runningConcurrency(s, e) AS rc FROM (SELECT * FROM events_04876 ORDER BY s);

SELECT 'ground truth', groupArray(if(i % 2 = 0, rc, 0)) FROM (SELECT * FROM running_04876 ORDER BY i);

-- The queries under test. All three must agree with the ground truth. `if` at the default
-- `short_circuit_function_evaluation = 'enable'` returned [1,0,2,0,3,0,4,0] before the fix.
SELECT 'if', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningConcurrency(s, e), 0) AS m FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'enable';

-- `optimize_multiif_to_if` is pinned off because it is on by default: a three-argument
-- `multiIf` is rewritten to `if`, so without the pin this arm re-tests the arm above.
SELECT 'multiIf', groupArray(m) FROM
    (SELECT multiIf(i % 2 = 0, runningConcurrency(s, e), 0) AS m FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'enable', optimize_multiif_to_if = 0;

SELECT 'and', groupArray(m) FROM
    (SELECT (i % 2 = 0) AND (runningConcurrency(s, e) > 2) AS m FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'enable';

-- Negative control: with short-circuiting off there is no lazy path to take, so this arm passed
-- before the fix too. It fails if the fixture stops overlapping or the ground truth drifts.
SELECT 'if disable', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningConcurrency(s, e), 0) AS m FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'disable';

-- Sibling control: a running function that already opted out of lazy execution behaves the same,
-- which is the precedent this fix follows.
SELECT 'sibling', groupArray(m) FROM
    (SELECT if(i % 2 = 0, rowNumberInAllBlocks(), 999) AS m FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'enable';

-- Still wrong on purpose: a computed argument puts the call back on the lazy path, so the
-- expected value below is the filtered one, not the ground truth.
SELECT 'computed argument, still filtered', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningConcurrency(toDateTime(toString(s)), toDateTime(toString(e))), 0) AS m
     FROM (SELECT * FROM events_04876 ORDER BY s))
SETTINGS short_circuit_function_evaluation = 'enable';

DROP TABLE running_04876;
DROP TABLE events_04876;
