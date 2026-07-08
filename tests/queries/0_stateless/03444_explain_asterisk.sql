-- Fuzzing `Can't set alias of * of Asterisk on alias`
DROP TABLE IF EXISTS t1, t2, t3__fuzz_0;
CREATE TABLE t1 (`a` UInt64, `b` UInt64) ENGINE = Log;
CREATE TABLE t2 (`a` UInt64, `b` UInt64) ENGINE = Log;
CREATE TABLE t3__fuzz_0 (`a` LowCardinality(UInt64), `b` UInt64) ENGINE = Log SETTINGS allow_suspicious_low_cardinality_types=1;

SET enable_analyzer = 0;
EXPLAIN SYNTAX  SELECT * FROM t1, t2, (SELECT toNullable(10), *, isZeroOrNull(10), 10, *, 10, *, *, 10, *, *, *, assumeNotNull(materialize(10)), 10 IS NOT NULL, a AS x FROM t3__fuzz_0 WHERE (toNullable(toUInt256(1)) + a) = b) AS t3 WHERE if(t2.b > 0, t2.a, 0) = t1.a ORDER BY t3.x ASC NULLS FIRST, t2.a DESC NULLS LAST, t1.a DESC NULLS FIRST;

SET enable_analyzer = 1;
EXPLAIN SYNTAX  SELECT * FROM t1, t2, (SELECT toNullable(10), *, isZeroOrNull(10), 10, *, 10, *, *, 10, *, *, *, assumeNotNull(materialize(10)), 10 IS NOT NULL, a AS x FROM t3__fuzz_0 WHERE (toNullable(toUInt256(1)) + a) = b) AS t3 WHERE if(t2.b > 0, t2.a, 0) = t1.a ORDER BY t3.x ASC NULLS FIRST, t2.a DESC NULLS LAST, t1.a DESC NULLS FIRST;
