DROP TABLE IF EXISTS t;

SET allow_suspicious_low_cardinality_types = 1;


CREATE TABLE t (`id` UInt16, `u` LowCardinality(Int32), `s` LowCardinality(String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t VALUES (1,1,'a'),(2,2,'b');

SELECT u, s FROM t
INNER JOIN ( SELECT number :: Int32 AS u FROM numbers(10) ) AS t1
USING (u)
WHERE u != 2
;

SELECT u, s, toTypeName(u) FROM t
FULL JOIN ( SELECT number :: UInt32 AS u FROM numbers(10) ) AS t1
USING (u)
WHERE u == 2
ORDER BY 1
;

DROP TABLE IF EXISTS t;
