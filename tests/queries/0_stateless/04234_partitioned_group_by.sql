-- { echo }

DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt32, b UInt32)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY a;

INSERT INTO t SELECT number % 8, number FROM numbers(1000);

SELECT intDiv(a, 2) AS g, sum(b)
FROM t
GROUP BY g
ORDER BY g
SETTINGS allow_aggregate_partitions_independently = 1,
         force_aggregate_partitions_independently = 1;

DROP TABLE t;
