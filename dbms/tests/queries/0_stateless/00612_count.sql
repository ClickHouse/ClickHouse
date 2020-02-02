DROP TABLE IF EXISTS test.count;

CREATE TABLE test.count (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test.count SELECT * FROM numbers(1234567);

SELECT count() FROM test.count;
SELECT count() * 2 FROM test.count;
SELECT count() FROM (SELECT * FROM test.count UNION ALL SELECT * FROM test.count);
SELECT count() FROM test.count WITH TOTALS;
SELECT arrayJoin([count(), count()]) FROM test.count;
SELECT arrayJoin([count(), count()]) FROM test.count LIMIT 1;
SELECT arrayJoin([count(), count()]) FROM test.count LIMIT 1, 1;
SELECT arrayJoin([count(), count()]) AS x FROM test.count LIMIT 1 BY x;
SELECT arrayJoin([count(), count() + 1]) AS x FROM test.count LIMIT 1 BY x;
SELECT count() FROM test.count HAVING count() = 1234567;
SELECT count() FROM test.count HAVING count() != 1234567;

DROP TABLE test.count;
