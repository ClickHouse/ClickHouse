-- Regression test: `runningAccumulate` must not be lazily executed as a short-circuit argument.
-- Lazy execution filters the argument column down to the rows the mask selects, so the function
-- would fold only the states of the surviving rows and report an accumulation computed over a
-- subset. Every row below contributes 1, so the value on a masked row differs from the value on
-- the same row of the full set, and the two answers cannot coincide by accident.

SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS states_05053;
DROP TABLE IF EXISTS running_05053;
DROP TABLE IF EXISTS grouped_05053;
DROP TABLE IF EXISTS running_g_05053;

-- The states are materialized instead of produced by `GROUP BY` in each query. `runningAccumulate`
-- restarts on every new block, so an aggregation upstream of it would make the expected values a
-- function of how that aggregation happened to be blocked: with `group_by_two_level_threshold = 1`
-- and `max_threads > 1` the two-level path emits several blocks and every value below changes.
-- Reading eight materialized rows keeps the input to a single block for any randomized
-- `max_block_size` (the runner draws it from 8000..100000).
CREATE TABLE states_05053 (i UInt32, st AggregateFunction(sum, UInt32)) ENGINE = Memory;
INSERT INTO states_05053 SELECT number, initializeAggregation('sumState', toUInt32(1)) FROM numbers(8);

-- Ground truth: the running value is materialized on its own, so no mask can reach it. The mask
-- is applied afterwards, to a value already accumulated over all eight rows.
CREATE TABLE running_05053 ENGINE = Memory AS
    SELECT i, runningAccumulate(st) AS r FROM (SELECT * FROM states_05053 ORDER BY i);

SELECT 'ground truth', groupArray(if(i % 2 = 0, r, 0)) FROM (SELECT * FROM running_05053 ORDER BY i);
SELECT 'ground truth and', groupArray((i % 2 = 0) AND (r > 2)) FROM (SELECT * FROM running_05053 ORDER BY i);
SELECT 'ground truth or', groupArray((i % 2 != 0) OR (r > 2)) FROM (SELECT * FROM running_05053 ORDER BY i);

-- The queries under test, one per function the setting covers. All four must agree with the
-- ground truth. Before the fix `if` and `multiIf` returned [1,0,2,0,3,0,4,0], `and` returned
-- [0,0,0,0,1,0,1,0] and `or` returned [0,1,0,1,1,1,1,1], so the boolean forms admitted a
-- different set of rows than the same predicate over the unmasked values.
SELECT 'if', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st), 0) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

-- `optimize_multiif_to_if` is pinned off because it is on by default: a three-argument `multiIf`
-- is rewritten to `if`, so without the pin this arm re-tests the arm above.
SELECT 'multiIf', groupArray(m) FROM
    (SELECT multiIf(i % 2 = 0, runningAccumulate(st), 0) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable', optimize_multiif_to_if = 0;

SELECT 'and', groupArray(m) FROM
    (SELECT (i % 2 = 0) AND (runningAccumulate(st) > 2) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'or', groupArray(m) FROM
    (SELECT (i % 2 != 0) OR (runningAccumulate(st) > 2) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

-- Negative control: with short-circuiting off there is no lazy path to take, so this arm passed
-- before the fix too. It fails if the fixture stops accumulating or the ground truth drifts.
SELECT 'if disable', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st), 0) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'disable';

-- Sibling control: a running function that already opted out of lazy execution behaves the same,
-- which is the precedent this fix follows.
SELECT 'sibling', groupArray(m) FROM
    (SELECT if(i % 2 = 0, rowNumberInAllBlocks(), 999) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

-- The two-argument overload behaves the same way, so the grouping argument does not reintroduce the
-- filtering: the accumulation restarts on every new group and the value on a masked row is the one
-- the unmasked run produced for that row. Before the fix this arm returned [1,0,2,0,1,0,2,0], the
-- accumulation of the four surviving rows split across the two groups.
CREATE TABLE grouped_05053 (i UInt32, g UInt8, st AggregateFunction(sum, UInt32)) ENGINE = Memory;
INSERT INTO grouped_05053 SELECT number, number >= 4, initializeAggregation('sumState', toUInt32(1)) FROM numbers(8);

CREATE TABLE running_g_05053 ENGINE = Memory AS
    SELECT i, runningAccumulate(st, g) AS r FROM (SELECT * FROM grouped_05053 ORDER BY i);

SELECT 'ground truth grouped', groupArray(if(i % 2 = 0, r, 0)) FROM (SELECT * FROM running_g_05053 ORDER BY i);

SELECT 'grouping column', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st, g), 0) AS m FROM (SELECT * FROM grouped_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

-- Still filtered on purpose: `force_enable` is documented as enabling lazy execution for all
-- functions, so it overrides the opt-out rather than consulting it.
SELECT 'force_enable, still filtered', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st), 0) AS m FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'force_enable';

-- Still filtered, and not a property of the overload above: a node is marked lazy when any argument
-- is itself lazy-eligible, which `has_lazy_child` decides before the opt-out is read. The arm after
-- it is the same query with no lazy path, so the value below is recorded as the current answer, not
-- claimed as the right one.
SELECT 'computed argument, still filtered', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st, toUInt8(toString(i % 1))), 0) AS m
     FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'computed argument, disable', groupArray(m) FROM
    (SELECT if(i % 2 = 0, runningAccumulate(st, toUInt8(toString(i % 1))), 0) AS m
     FROM (SELECT * FROM states_05053 ORDER BY i))
SETTINGS short_circuit_function_evaluation = 'disable';

-- The same declaration also feeds `canThrow`, which decides whether the rows a replicated argument
-- does not reference are dropped before execution. Finalizing an arbitrary aggregate state can
-- throw, so the unreferenced state below must not be finalized: the k = 0 state has an empty second
-- sample and raises BAD_ARGUMENTS if it is, while ARRAY JOIN leaves it unreferenced.
DROP TABLE IF EXISTS states_ks_05053;
CREATE TABLE states_ks_05053 (k UInt8, st AggregateFunction(kolmogorovSmirnovTest, Float64, UInt8), arr Array(UInt8)) ENGINE = Memory;
INSERT INTO states_ks_05053 SELECT k, kolmogorovSmirnovTestState(x, sample), if(k = 0, CAST([], 'Array(UInt8)'), [toUInt8(1)])
    FROM values('k UInt8, x Float64, sample UInt8', (0, 0.1, 0), (1, 0.2, 0), (1, 0.3, 1)) GROUP BY k;

SELECT 'unreferenced replicated state', runningAccumulate(st) FROM
    (SELECT st FROM (SELECT * FROM states_ks_05053 ORDER BY k) ARRAY JOIN arr LIMIT 100)
SETTINGS enable_lazy_columns_replication = 1;

-- Opting out of lazy execution also means the call is evaluated when the mask selects no rows at
-- all, so a state that cannot be finalized raises instead of being skipped. That is the same
-- answer `short_circuit_function_evaluation = 'disable'` has always given, and the same answer the
-- other row-order-dependent functions give; `force_enable` keeps the branch unevaluated because it
-- overrides the opt-out.
DROP TABLE IF EXISTS unused_05053;
CREATE TABLE unused_05053 (flag UInt8, st AggregateFunction(kolmogorovSmirnovTest, Float64, UInt8)) ENGINE = Memory;
INSERT INTO unused_05053 SELECT 0, kolmogorovSmirnovTestState(x, sample) FROM values('x Float64, sample UInt8', (0.1, 0));

SELECT if(flag, runningAccumulate(st).1, 42) FROM unused_05053
SETTINGS short_circuit_function_evaluation = 'enable'; -- { serverError BAD_ARGUMENTS }

SELECT 'unused branch, force_enable', if(flag, runningAccumulate(st).1, 42) FROM unused_05053
SETTINGS short_circuit_function_evaluation = 'force_enable';

DROP TABLE unused_05053;
DROP TABLE states_ks_05053;
DROP TABLE running_g_05053;
DROP TABLE grouped_05053;
DROP TABLE running_05053;
DROP TABLE states_05053;
