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

-- Single-conjunct case: move_all_conditions_to_prewhere = 0 pushes only (42 >= id) to
-- PREWHERE, so prewhere_column_name is that predicate itself and the residual WHERE
-- (value != 7) still consumes it. The set build must keep, not remove, that column.
SELECT count()
FROM t_04493 FINAL
WHERE (42 >= id) AND (value != 7)
SETTINGS move_all_conditions_to_prewhere = 0;

SELECT count()
FROM t_04493 FINAL
WHERE (42 >= id) AND (value != 7)
SETTINGS move_all_conditions_to_prewhere = 0, query_plan_optimize_lazy_final = 0;

DROP TABLE t_04493;

-- Plain-storage-column single-conjunct case: the pushed PREWHERE predicate is a plain
-- storage column (flag), not a derived/computed predicate. With move_all_conditions_to_prewhere = 0
-- only `flag` is pushed and the residual WHERE (value != 7 AND flag) still consumes it, so
-- splitAndFillPrewhereInfo sets remove_prewhere_column = false. The set build must keep that
-- column: recomputing removal from derived-only inputs erased flag and threw NOT_FOUND_COLUMN_IN_BLOCK.
DROP TABLE IF EXISTS t_04493_plain;

CREATE TABLE t_04493_plain
(
    id UInt64,
    update_ts DateTime,
    flag UInt8,
    value UInt32
)
ENGINE = ReplacingMergeTree(update_ts)
PARTITION BY 0 * id
ORDER BY (flag, id);

INSERT INTO t_04493_plain SELECT number, toDateTime('2020-01-01 00:00:00'), 1, 1 FROM numbers(100);
INSERT INTO t_04493_plain SELECT number, toDateTime('2020-01-02 00:00:00'), 1, 2 FROM numbers(100);

SELECT count()
FROM t_04493_plain FINAL
WHERE flag AND (value != 7)
SETTINGS move_all_conditions_to_prewhere = 0;

SELECT count()
FROM t_04493_plain FINAL
WHERE flag AND (value != 7)
SETTINGS move_all_conditions_to_prewhere = 0, query_plan_optimize_lazy_final = 0;

DROP TABLE t_04493_plain;
