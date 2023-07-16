DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int16, projection h (SELECT min(c0), max(c0), count() GROUP BY -c0)) ENGINE = MergeTree ORDER BY ();

INSERT INTO t0(c0) VALUES (1);

SELECT count() FROM t0 GROUP BY gcd(-sign(c0), -c0);

DROP TABLE t0;
