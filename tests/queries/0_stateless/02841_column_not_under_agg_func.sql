CREATE TABLE test_02841(dt DateTime, n Int64) ENGINE=Memory()
SELECT timezoneOf(dt) tz, count(*) FROM test_02841 GROUP BY tz
DROP TABLE test_02841
