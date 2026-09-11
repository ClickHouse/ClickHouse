-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100469 (STID: 1499-4a82).
--
-- The JOIN-conversion optimizers `tryConvertAnyOuterJoinToInnerJoin` (`convertOuterJoinToInnerJoin.cpp`)
-- and `tryConvertAnyJoinToSemiOrAntiJoin` (`convertAnyJoinToSemiOrAntiJoin.cpp`) call
-- `filterResultForNotMatchedRows` / `filterResultForMatchedRows` (in
-- `Processors/QueryPlan/Optimizations/Utils.cpp` and `convertAnyJoinToSemiOrAntiJoin.cpp`)
-- to evaluate the filter against synthetic default-input rows via `ActionsDAG::evaluatePartialResult`
-- with `skip_materialize=true, allow_unknown_function_arguments=true`. Internally this calls
-- `IFunction::executeImplDryRun` for every function in the filter DAG.
--
-- Two related bugs are covered here:
--
--   1. `MemorySanitizer` `use-of-uninitialized-value`: the previous dry-run override of
--      `rowNumberInAllBlocks` returned `ColumnUInt64::create(input_rows_count)` without filling
--      the buffer, so when the optimizer read the result via `getFilterResult` -> `getBool(0)`
--      MSan reported a use-of-uninitialized-value. Fixed at the source by initializing the
--      dry-run output column with zeros (`Functions/rowNumberInAllBlocks.cpp`).
--
--   2. Soundness: even with an initialized `[0]` output, the optimizer cannot soundly predict
--      the filter result for non-deterministic functions. For a filter such as
--      `rowNumberInAllBlocks() = 1`, the dry-run row evaluates to `0 = 1` -> FALSE; the
--      optimizer then concludes that filter is FALSE for all not-matched rows and converts
--      `ANY LEFT JOIN` to `SEMI JOIN`, silently dropping unmatched rows that would have
--      satisfied the filter at runtime. Fixed by bailing out to `FilterResult::UNKNOWN`
--      when the filter DAG contains any function whose `isDeterministicInScopeOfQuery` returns false.
--
-- Both bugs are in the planner-based path (`Processors/QueryPlan/Optimizations/`). The old
-- analyzer does not run these JOIN-conversion optimizers, and it also rejects the scalar
-- subquery shape `(SELECT p = 1)` from `PedroTadim`'s reproducer with `UNKNOWN_IDENTIFIER`
-- in `TreeRewriter::collectUsedColumns`. So we force the new analyzer for this whole test;
-- this matches sibling JOIN-conversion regression tests (03130, 03210, 03595, 03623, 04003).
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);

-- ANY LEFT JOIN with a filter that contains `rowNumberInAllBlocks` (stateful, non-deterministic).
-- Both rows from t_l must be returned; the unmatched row gets default 0 from t_r.
-- Without the fix, the optimizer reads uninitialized memory from the dry-run output of
-- `rowNumberInAllBlocks` while deciding whether to convert ANY OUTER JOIN to INNER JOIN.
SELECT t_l.id, t_r.id
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() < 100
ORDER BY t_l.id;

-- ANY LEFT JOIN where the filter is FALSE on the dry-run row (`rowNumberInAllBlocks()` is 0
-- in dry-run, so `0 = 1` -> FALSE) but TRUE for some row at runtime. The unmatched row must
-- not be silently dropped by `tryConvertAnyOuterJoinToInnerJoin` /
-- `tryConvertAnyJoinToSemiOrAntiJoin`. Without the optimizer guard the optimizer converts
-- the JOIN to SEMI/INNER and the count drops to 0 (silent data loss); with the guard the
-- JOIN is left as ANY LEFT and exactly one row passes the `rowNumberInAllBlocks() = 1`
-- filter. We assert the row count rather than the specific row to stay robust under
-- randomized thread settings (the value of `rowNumberInAllBlocks` depends on per-stream
-- block ordering).
SELECT count()
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() = 1;

-- Symmetric ANY RIGHT JOIN -- exercises the right-stream branch of `tryConvertAnyOuterJoinToInnerJoin`.
DROP TABLE t_l;
DROP TABLE t_r;
CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1);
INSERT INTO t_r VALUES (1), (2);

SELECT t_l.id, t_r.id
FROM t_l ANY RIGHT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() < 100
ORDER BY t_r.id;

-- ANY RIGHT JOIN soundness companion: filter is FALSE on dry-run row (`0 = 1`) but TRUE for
-- some runtime row. Same reasoning as above; assert the row count rather than the specific
-- row to remain robust under randomized thread settings.
SELECT count()
FROM t_l ANY RIGHT JOIN t_r ON t_l.id = t_r.id
WHERE rowNumberInAllBlocks() = 1;

-- ANY LEFT JOIN with `rand` in the filter -- another non-deterministic function whose dry-run
-- output flows into the same partial-evaluation chain. The filter is always true (the modulus is
-- non-negative), so both rows from t_l must be kept.
DROP TABLE t_l;
DROP TABLE t_r;
CREATE TABLE t_l (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_r (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_l VALUES (1), (2);
INSERT INTO t_r VALUES (1);

SELECT t_l.id, t_r.id
FROM t_l ANY LEFT JOIN t_r ON t_l.id = t_r.id
WHERE (rand() % 1000) >= 0
ORDER BY t_l.id;

-- PedroTadim's original simplified reproducer (issue #100469): no explicit JOIN, but
-- exercises a similar partial-evaluation chain. Must run cleanly without MSan reports.
DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (p Int) ENGINE = Memory;
INSERT INTO t0 VALUES (0);
SELECT 1 FROM t0 WHERE (SELECT p = 1) AND rowNumberInAllBlocks() = 1;

DROP TABLE t0;
