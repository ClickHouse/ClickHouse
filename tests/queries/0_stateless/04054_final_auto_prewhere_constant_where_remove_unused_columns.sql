-- Test for block structure mismatch in removeUnusedColumns when combining
-- FINAL + auto-PREWHERE (optimize_move_to_prewhere_if_final=1) + constant WHERE expression.
--
-- When optimize_move_to_prewhere_if_final=1, the optimizer automatically promotes
-- eligible WHERE conditions to PREWHERE during the optimizePrewhere second pass.
-- A WHERE expression that uses no table columns causes FilterStep to declare zero
-- required input columns, while ReadFromMergeTree with FINAL must keep sort key
-- columns {ts, id} for the merge. absorbExtraChildColumns must bridge this gap;
-- without the fix it left a column-count mismatch causing a LOGICAL_ERROR.

DROP TABLE IF EXISTS t_final_auto_prewhere;

CREATE TABLE t_final_auto_prewhere (id UInt64, ts DateTime, value UInt32, extra UInt64)
ENGINE = ReplacingMergeTree(ts) PARTITION BY 0 * id ORDER BY (ts, id);

INSERT INTO t_final_auto_prewhere
SELECT number, toDateTime('2020-01-01 00:00:00'), 1, number FROM numbers(100);

-- Variant 1: auto-PREWHERE path with constant WHERE and aggregate
SELECT count() FROM t_final_auto_prewhere FINAL
WHERE (42 >= id) AND ('2021-01-01' <= ts) AND equals(and(8, 8), 8)
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1;

-- Variant 2: same but with window function (adds WindowStep downstream)
SELECT count() OVER () FROM t_final_auto_prewhere FINAL
WHERE (42 >= id) AND ('2021-01-01' <= ts) AND equals(and(8, 8), 8)
SETTINGS optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1;

DROP TABLE t_final_auto_prewhere;
