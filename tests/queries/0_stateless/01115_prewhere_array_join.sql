DROP TABLE IF EXISTS prewhere;

CREATE TABLE prewhere (light UInt8, heavy String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO prewhere SELECT 0, randomPrintableASCII(10000) FROM numbers(10000);
SELECT arrayJoin([light]) != 0 AS cond, length(heavy) FROM prewhere WHERE light != 0 AND cond != 0;

DROP TABLE prewhere;

DROP TABLE IF EXISTS testtable;
CREATE TABLE testtable (DT Datetime, Label1 String, Value UInt64) ENGINE = MergeTree() PARTITION BY DT ORDER BY Label1;
INSERT INTO testtable (*) Values (now(), 'app', 1);
SELECT arrayJoin([0, 1]) AS arrayIdx FROM testtable WHERE arrayIdx = 0;
DROP TABLE testtable;
