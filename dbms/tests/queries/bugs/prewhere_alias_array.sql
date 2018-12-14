DROP TABLE IF EXISTS test.prewhere;
CREATE TABLE test.prewhere (x Array(UInt64), y ALIAS x, s String) ENGINE = MergeTree ORDER BY tuple()
SELECT count() FROM test.prewhere PREWHERE (length(s) >= 1) = 0 WHERE NOT ignore(y)
DROP TABLE test.prewhere;
