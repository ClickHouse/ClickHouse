-- { echo }

DROP TABLE IF EXISTS t;
CREATE TABLE t (a Nullable(UInt32), b UInt32, s String, arr Array(UInt32))
ENGINE = MergeTree ORDER BY b SETTINGS allow_nullable_key = 1;

INSERT INTO t VALUES (1, 10, 'x', [100, 200]), (2, 20, 'y', [300, 400]), (NULL, 30, 'z', [500, 600]);

SELECT count() FROM (SELECT x FROM t ARRAY JOIN arr AS x LIMIT -1 BY a);

SELECT count() FROM (SELECT x FROM t ARRAY JOIN arr AS x ORDER BY a LIMIT -1 BY a);

SELECT count() FROM (SELECT x FROM t ARRAY JOIN arr AS x LIMIT 1 BY a);

SELECT count() FROM (SELECT x FROM t ARRAY JOIN arr AS x ORDER BY a LIMIT 1 BY a);

DROP TABLE t;
