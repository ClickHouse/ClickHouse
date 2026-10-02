-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/82279
-- The `optimizePrimaryKeyConditionAndLimit` pass walks from a
-- `SourceStepWithFilterBase` through `ExpressionStep`s and propagates the
-- outer `LimitStep` cap into the source via `setLimit`. When an
-- `ExpressionStep` on that walk contains `arrayJoin`, the source is told to
-- generate at most N input rows; `arrayJoin` then expands only those input
-- rows, which can produce far fewer than N output rows when the early input
-- rows have empty arrays. The query plan does not need a `SortingStep` to
-- trigger the bug, only `LIMIT` above an `ExpressionStep` containing
-- `arrayJoin` above any `SourceStepWithFilterBase`.

-- Bot's exact repro: rows 0..2 produce empty arrays via `if`, so a source
-- limit of 3 pre-`arrayJoin` truncates to numbers 0..2 and `arrayJoin`
-- expands no rows. Without the source limit, the source generates enough
-- numbers for `arrayJoin` to produce at least 3 output rows.
SELECT '-- bot repro: arrayJoin with empty-array prefix';
SELECT arrayJoin(if(number < 3, [], [number])) FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- bot repro, old analyzer';
SELECT arrayJoin(if(number < 3, [], [number])) FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 0;

-- The same bug shape but with a heterogeneous expansion: rows 0..1 produce
-- two elements each, so a pre-`arrayJoin` source limit of 3 produces only
-- 2 + 2 + 1 = 5 rows but the user asked for `LIMIT 3` of *expanded* output,
-- which is `(0,0), (0,10), (1,1)`. The bug truncates source rows, the fix
-- truncates output rows.
SELECT '-- multi-element arrayJoin';
SELECT number, arrayJoin([number, number + 10]) FROM numbers(100) LIMIT 3 SETTINGS allow_experimental_analyzer = 1;

-- Same bug on a `MergeTree` source (where `optimizePrimaryKeyConditionAndLimit`
-- also fires) - empty arrays for the first sorted rows used to drop output.
DROP TABLE IF EXISTS t_arrayjoin_limit_no_sort;
CREATE TABLE t_arrayjoin_limit_no_sort (id UInt32, a Array(UInt16)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arrayjoin_limit_no_sort VALUES (1, []), (2, []), (3, []), (4, [10, 20, 30, 40, 50]), (5, []);

SELECT '-- MergeTree, no ORDER BY, LIMIT 3';
SELECT count() FROM (SELECT id, arrayJoin(a) FROM t_arrayjoin_limit_no_sort LIMIT 3);

DROP TABLE t_arrayjoin_limit_no_sort;

-- A different hard-source-limit path: `maxBlockSizeByLimit` /
-- `mainQueryNodeBlockSizeByLimit` set `query_info.trivial_limit`, which
-- `ReadFromLoopStep::generate` (and system.zeros, generateRandom) consume as a
-- HARD generation cap, before the outer `arrayJoin` expansion runs. This pass
-- runs during planning, not as a query-plan optimization, so the bot's
-- `loop()` repro reproduces even with `query_plan_enable_optimizations = 0`.
SELECT '-- loop() source, LIMIT 3';
SELECT arrayJoin(if(number < 3, [], [number])) FROM loop(numbers(100)) LIMIT 3 SETTINGS allow_experimental_analyzer = 1;
SELECT '-- loop() source, old analyzer';
SELECT arrayJoin(if(number < 3, [], [number])) FROM loop(numbers(100)) LIMIT 3 SETTINGS allow_experimental_analyzer = 0;
SELECT '-- loop() source, optimizations disabled';
SELECT arrayJoin(if(number < 3, [], [number])) FROM loop(numbers(100)) LIMIT 3 SETTINGS query_plan_enable_optimizations = 0;
SELECT '-- loop() source, LIMIT 3 OFFSET 1';
SELECT arrayJoin(if(number < 3, [], [number])) FROM loop(numbers(100)) LIMIT 3 OFFSET 1;
SELECT '-- loop() source, ARRAY JOIN clause, old analyzer';
SELECT x FROM loop(numbers(100)) ARRAY JOIN if(number < 3, [], [number]) AS x LIMIT 3 SETTINGS allow_experimental_analyzer = 0;

-- The trivial-LIMIT optimization must still fire when there is no `arrayJoin`:
-- a plain `loop()` read is an infinite source, so `LIMIT 3` only terminates
-- because the source is hard-capped at 3 rows.
SELECT '-- loop() without arrayJoin still terminates (optimization fires)';
SELECT number FROM loop(numbers(100)) LIMIT 3;
SELECT '-- unreferenced WITH arrayJoin does not block pushdown';
WITH arrayJoin([10, 20, 30]) AS unused SELECT number FROM loop(numbers(100)) LIMIT 3;
