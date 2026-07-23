-- Test: DELETE WHERE with various predicate forms (equality, range, string, compound).

DROP TABLE IF EXISTS t_lwd_pred;

CREATE TABLE t_lwd_pred (a UInt32, b String, c Float64)
    ENGINE = MergeTree ORDER BY a;

INSERT INTO t_lwd_pred SELECT number, toString(number), number * 0.5 FROM numbers(100);

SET lightweight_deletes_sync = 2;

-- Simple equality predicate
DELETE FROM t_lwd_pred WHERE a = 50;
SELECT count() = 99 FROM t_lwd_pred;
SELECT count() = 0 FROM t_lwd_pred WHERE a = 50;

-- Range predicate
DELETE FROM t_lwd_pred WHERE a BETWEEN 10 AND 19;
SELECT count() = 89 FROM t_lwd_pred;
SELECT count() = 0 FROM t_lwd_pred WHERE a BETWEEN 10 AND 19;

-- String predicate
DELETE FROM t_lwd_pred WHERE b = '99';
SELECT count() = 88 FROM t_lwd_pred;

-- Compound predicate
DELETE FROM t_lwd_pred WHERE a < 5 OR a > 90;
SELECT count() FROM t_lwd_pred;

-- Float comparison
DELETE FROM t_lwd_pred WHERE c > 40.0 AND c < 45.0;
SELECT count() FROM t_lwd_pred;

-- All remaining row values must be intact
SELECT count() = 0 FROM t_lwd_pred WHERE c != a * 0.5;

DROP TABLE t_lwd_pred;
