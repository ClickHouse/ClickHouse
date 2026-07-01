-- Regression test for issue #109059: query_plan_optimize_lazy_final built the
-- set-building ReadFromMergeTree with a derived (prewhere-produced) column name in
-- its storage column list, throwing NOT_FOUND_COLUMN_IN_BLOCK.
--
-- optimize_move_to_prewhere_if_final splits the WHERE so the reading step's prewhere
-- produces the computed predicate greaterOrEquals(42, id) and the remaining WHERE
-- stays as a FilterStep above the reading step consuming it. The lazy FINAL set build
-- must not request that derived column from storage. optimize_on_insert = 0 keeps the
-- inserted rows un-deduplicated so FINAL takes the intersecting (real FINAL) path.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_final = 1;
SET optimize_move_to_prewhere_if_final = 1;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_04493;

CREATE TABLE t_04493
(
    id UInt64,
    update_ts DateTime,
    value UInt32
)
ENGINE = ReplacingMergeTree(update_ts)
PARTITION BY 0 * id
ORDER BY (update_ts, id);

INSERT INTO t_04493
SELECT number, toDateTime('2020-01-01 00:00:00'), 1
FROM numbers(100);

-- equals(and(8, 8), 1) is a runtime (not constant-folded) predicate, so the WHERE
-- FilterStep is kept above the reading step and references the prewhere-produced
-- predicate columns. Before the fix this threw NOT_FOUND_COLUMN_IN_BLOCK.
SELECT count()
FROM t_04493 FINAL
WHERE (42 >= id) AND ('2019-01-01 00:00:00' <= update_ts) AND equals(and(8, 8), 1);

-- Result must match with the optimization disabled.
SELECT count()
FROM t_04493 FINAL
WHERE (42 >= id) AND ('2019-01-01 00:00:00' <= update_ts) AND equals(and(8, 8), 1)
SETTINGS query_plan_optimize_lazy_final = 0;

DROP TABLE t_04493;
