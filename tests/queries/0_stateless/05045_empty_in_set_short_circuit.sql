-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- `PartsSplitter` fault injection reads the granule on each layer border twice, which changes `read_rows`.

-- an empty set behind IN (subquery) must skip the read, Nullable column included
-- prewhere is pinned off so the condition lands in a filter above the read, which is what folds it
DROP TABLE IF EXISTS t_short_circuit;
DROP TABLE IF EXISTS t_short_circuit_final;
DROP TABLE IF EXISTS t_short_circuit_set;
DROP TABLE IF EXISTS t_short_circuit_set_engine;

CREATE TABLE t_short_circuit (a UInt64, b Nullable(UInt64)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_short_circuit_final (a UInt64, b Nullable(UInt64)) ENGINE = ReplacingMergeTree ORDER BY a;
CREATE TABLE t_short_circuit_set (b UInt64) ENGINE = MergeTree ORDER BY b;
CREATE TABLE t_short_circuit_set_engine (b UInt64) ENGINE = Set;

INSERT INTO t_short_circuit SELECT number, number FROM numbers(100000);
INSERT INTO t_short_circuit_final SELECT number, number FROM numbers(100000);

SET optimize_move_to_prewhere = 0;

SELECT count() /* assert_no_read */ FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set);
SELECT count() /* assert_no_read */ FROM t_short_circuit_final FINAL WHERE b IN (SELECT b FROM t_short_circuit_set);
-- transform_null_in rewrites IN to nullIn, which must short-circuit as well
SELECT count() /* assert_no_read */ FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set) SETTINGS transform_null_in = 1;
-- a conjunct is enough to make the whole filter false; read_rows is not asserted here because
-- whether this lands in a filter or inside the read step depends on the plan
SELECT count() FROM t_short_circuit WHERE b IN (SELECT b FROM t_short_circuit_set) AND a > 10;
-- NOT IN over an empty set matches everything, so this one reads the whole table
SELECT count() FROM t_short_circuit WHERE b NOT IN (SELECT b FROM t_short_circuit_set);
-- the set of an ENGINE = Set table can grow while the query runs, so an empty one still reads the whole table
SELECT count() /* assert_full_read */ FROM t_short_circuit WHERE b IN t_short_circuit_set_engine;
SELECT count() /* assert_full_read */ FROM t_short_circuit WHERE b IN t_short_circuit_set_engine SETTINGS transform_null_in = 1;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE '%assert_no_read%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE '%assert_full_read%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE t_short_circuit;
DROP TABLE t_short_circuit_final;
DROP TABLE t_short_circuit_set;
DROP TABLE t_short_circuit_set_engine;
