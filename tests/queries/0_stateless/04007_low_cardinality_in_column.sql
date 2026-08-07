-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/92828
-- LowCardinality column with IN (column_ref) used to throw
-- "Nested type LowCardinality(...) cannot be inside Nullable type"

SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_lc_in;
CREATE TABLE t_lc_in (a LowCardinality(String), b String) ENGINE = Memory;
INSERT INTO t_lc_in VALUES ('x', 'x'), ('y', 'z');

SELECT a, b, a IN (b) FROM t_lc_in ORDER BY a;
SELECT a, b, a NOT IN (b) FROM t_lc_in ORDER BY a;

DROP TABLE t_lc_in;

-- Also test with LowCardinality(Bool)
CREATE TABLE t_lc_in (a LowCardinality(Bool), b Bool) ENGINE = Memory;
INSERT INTO t_lc_in VALUES (true, true), (false, false), (true, false);

SELECT a, b, a IN (b) FROM t_lc_in ORDER BY a, b;
SELECT a, b, a NOT IN (b) FROM t_lc_in ORDER BY a, b;

DROP TABLE t_lc_in;
