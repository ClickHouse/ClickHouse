DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS events32;

CREATE TABLE calendar ( `year` Int64, `month` Int64 ) ENGINE = TinyLog;
INSERT INTO calendar VALUES (2000, 1), (2001, 2), (2000, 3);

CREATE TABLE events32 ( `year` Int32, `month` Int32 ) ENGINE = TinyLog;
INSERT INTO events32 VALUES (2001, 2), (2001, 3);

SELECT * FROM calendar WHERE (year, month) IN ( SELECT (year, month) FROM events32 );

DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS events32;
