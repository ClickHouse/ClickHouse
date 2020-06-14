DROP TABLE IF EXISTS count;

CREATE TABLE count (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO count SELECT * FROM numbers(1234567);

SELECT count() FROM count;
SELECT count() * 2 FROM count;
SELECT count() FROM (SELECT * FROM count UNION ALL SELECT * FROM count);
SELECT count() FROM count WITH TOTALS;
SELECT arrayJoin([count(), count()]) FROM count;
SELECT arrayJoin([count(), count()]) FROM count LIMIT 1;
SELECT arrayJoin([count(), count()]) FROM count LIMIT 1, 1;
SELECT arrayJoin([count(), count()]) AS x FROM count LIMIT 1 BY x;
SELECT arrayJoin([count(), count() + 1]) AS x FROM count LIMIT 1 BY x;
SELECT count() FROM count HAVING count() = 1234567;
SELECT count() FROM count HAVING count() != 1234567;

DROP TABLE count;
