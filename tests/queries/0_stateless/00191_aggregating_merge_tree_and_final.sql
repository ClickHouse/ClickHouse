DROP TABLE IF EXISTS aggregating_00191;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE aggregating_00191 (d Date DEFAULT '2000-01-01', k UInt64, u AggregateFunction(uniq, UInt64)) ENGINE = AggregatingMergeTree(d, k, 8192);

INSERT INTO aggregating_00191 (k, u) SELECT intDiv(number, 100) AS k, uniqState(toUInt64(number % 100)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000) GROUP BY k;
INSERT INTO aggregating_00191 (k, u) SELECT intDiv(number, 100) AS k, uniqState(toUInt64(number % 100) + 50) AS u FROM (SELECT * FROM system.numbers LIMIT 500, 1000) GROUP BY k;

SELECT k, finalizeAggregation(u) FROM aggregating_00191 FINAL order by k;

OPTIMIZE TABLE aggregating_00191;

SELECT k, finalizeAggregation(u) FROM aggregating_00191;
SELECT k, finalizeAggregation(u) FROM aggregating_00191 FINAL order by k;

DROP TABLE aggregating_00191;
