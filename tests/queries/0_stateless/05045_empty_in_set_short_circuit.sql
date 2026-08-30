-- an empty set behind IN (subquery) must skip the read, Nullable column included
-- prewhere is pinned off so the condition lands in a filter above the read, which is what folds it
DROP TABLE IF EXISTS t_short_circuit;
DROP TABLE IF EXISTS t_short_circuit_final;
DROP TABLE IF EXISTS t_short_circuit_set;

CREATE TABLE t_short_circuit (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_short_circuit_final (a UInt64, b Nullable(UInt64)) ENGINE = ReplacingMergeTree ORDER BY a;
CREATE TABLE t_short_circuit_set (b UInt64) ENGINE = MergeTree ORDER BY b;

INSERT INTO t_short_circuit SELECT number, number FROM numbers(100000);
INSERT INTO t_short_circuit_final SELECT number, number FROM numbers(100000);

SET optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

SELECT count() /* assert_no_read */ FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set);
SELECT count() /* assert_no_read */ FROM t_short_circuit_final FINAL WHERE b IN (SELECT b FROM t_short_circuit_set);
-- transform_null_in rewrites IN to nullIn, which must short-circuit as well
SELECT count() /* assert_no_read */ FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set) SETTINGS transform_null_in = 1;
-- a conjunct is enough to make the whole filter false; read_rows is not asserted here because
-- whether this lands in a filter or inside the read step depends on the plan
SELECT count() FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set) AND a > 10;
-- NOT IN over an empty set matches everything, so this one reads the whole table
SELECT count() FROM t_short_circuit WHERE b NOT IN (SELECT b FROM t_short_circuit_set);

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE '%assert_no_read%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_short_circuit;
DROP TABLE t_short_circuit_final;
DROP TABLE t_short_circuit_set;
