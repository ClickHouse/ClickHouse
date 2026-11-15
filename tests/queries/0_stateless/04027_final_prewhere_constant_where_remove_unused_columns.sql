-- Test for block structure mismatch in removeUnusedColumns when combining
-- FINAL + PREWHERE + constant WHERE expression + aggregate with no column dependencies.
-- The WHERE expression is constant (evaluates without any table columns), so the Filter step
-- needs zero input columns, but ReadFromMergeTree with FINAL cannot reduce its output
-- below the columns required for merging. This must not cause a LOGICAL_ERROR.

DROP TABLE IF EXISTS t_final_prewhere_const_where;

CREATE TABLE t_final_prewhere_const_where (id UInt64, update_ts DateTime, value UInt32)
ENGINE = ReplacingMergeTree(update_ts) PARTITION BY 0 * id ORDER BY (update_ts, id);

INSERT INTO t_final_prewhere_const_where SELECT number, toDateTime('2020-01-01 00:00:00'), 1 FROM numbers(100);

-- The and(8, 8) expression is constant but uses the `and` function which interacts with optimizer differently from plain literals
SELECT count() FROM t_final_prewhere_const_where FINAL
    PREWHERE (42 >= id) AND ('2021-01-01 00:00:00' <= update_ts)
    WHERE equals(and(8, 8), 8);

SELECT DISTINCT count() FROM t_final_prewhere_const_where FINAL
    PREWHERE (42 >= id) AND ('2021-01-01 00:00:00' <= update_ts)
    WHERE equals(and(toUInt32(toUInt128(8)), assumeNotNull(8)), 8);

DROP TABLE t_final_prewhere_const_where;
