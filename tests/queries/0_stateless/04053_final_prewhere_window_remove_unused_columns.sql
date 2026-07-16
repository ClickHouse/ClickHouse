-- Test for block structure mismatch in removeUnusedColumns when combining
-- FINAL + PREWHERE + constant WHERE expression + window function.
--
-- A WHERE expression that uses no table columns (all inputs are constants)
-- causes FilterStep to declare zero required input columns.
-- ReadFromMergeTree with FINAL must keep the sort key columns for the merge
-- ({update_ts, id}), so its output cannot be reduced to zero columns.
-- absorbExtraChildColumns() must bridge this gap; without the fix it left
-- a column-count mismatch that surfaced as a block structure error when a
-- WindowStep was downstream of the FilterStep.

DROP TABLE IF EXISTS t_final_prewhere_wnd;

CREATE TABLE t_final_prewhere_wnd (id UInt64, update_ts DateTime, value UInt32)
ENGINE = ReplacingMergeTree(update_ts) PARTITION BY 0 * id ORDER BY (update_ts, id);

INSERT INTO t_final_prewhere_wnd SELECT number, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(100);

-- The and(8, 8) expression is constant but uses the `and` function, which
-- interacts with the optimizer differently from plain literals.  FilterStep
-- therefore keeps zero table-column inputs while ReadFromMergeTree (FINAL)
-- must keep {update_ts, id} for sort-merge -- a mismatch that must be resolved
-- without a block structure error.

-- Variant with aggregate (no window step):
SELECT count() FROM t_final_prewhere_wnd FINAL
    PREWHERE (42 >= id) AND ('2021-01-01 00:00:00' <= update_ts)
    WHERE equals(and(8, 8), 8);

-- Variant with window function (adds WindowStep downstream of FilterStep):
SELECT count() OVER () FROM t_final_prewhere_wnd FINAL
    PREWHERE (42 >= id) AND ('2021-01-01 00:00:00' <= update_ts)
    WHERE equals(and(8, 8), 8);

-- Variant with DISTINCT to exercise a different plan shape:
SELECT DISTINCT count() FROM t_final_prewhere_wnd FINAL
    PREWHERE (42 >= id) AND ('2021-01-01 00:00:00' <= update_ts)
    WHERE equals(and(toUInt32(toUInt128(8)), assumeNotNull(8)), 8);

DROP TABLE t_final_prewhere_wnd;
