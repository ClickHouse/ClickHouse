DROP TABLE IF EXISTS aggregating;
CREATE TABLE aggregating (d Date DEFAULT '2000-01-01', k UInt64, u AggregateFunction(uniq, UInt64)) ENGINE = AggregatingMergeTree(d, k, 8192);

INSERT INTO aggregating (k, u) SELECT intDiv(number, 100) AS k, uniqState(toUInt64(number % 100)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000) GROUP BY k;
INSERT INTO aggregating (k, u) SELECT intDiv(number, 100) AS k, uniqState(toUInt64(number % 100) + 50) AS u FROM (SELECT * FROM system.numbers LIMIT 500, 1000) GROUP BY k;

SELECT k, finalizeAggregation(u) FROM aggregating FINAL;

OPTIMIZE TABLE aggregating;

SELECT k, finalizeAggregation(u) FROM aggregating;
SELECT k, finalizeAggregation(u) FROM aggregating FINAL;

DROP TABLE aggregating;
