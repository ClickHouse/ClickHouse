-- Regression test: reading different subcolumns of a Nested column from Log engine
-- could produce chunks with inconsistent column sizes because the shared Nested
-- offsets file was read twice from different stream positions.

DROP TABLE IF EXISTS t_log_nested;
CREATE TABLE t_log_nested
(
    id UInt64,
    n Nested(a UInt64, b Nullable(FixedString(1)))
) ENGINE = Log;

INSERT INTO t_log_nested SELECT number, [number, number + 1], [NULL, 'x'] FROM numbers(32);

-- This query reads n.a.size0 (shared Nested offsets) and n.b subcolumn
-- from the same Nested group. They must share the SubstreamsCache
-- to avoid reading the shared offsets file twice.
SELECT n.a.size0, n.b FROM t_log_nested FORMAT Null;

-- Also test with multiple inserts (marks-based reading)
INSERT INTO t_log_nested SELECT number, [number], [NULL] FROM numbers(32);
SELECT n.a.size0, n.b FROM t_log_nested FORMAT Null;

DROP TABLE t_log_nested;
