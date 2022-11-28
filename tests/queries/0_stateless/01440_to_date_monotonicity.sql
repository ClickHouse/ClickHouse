DROP TABLE IF EXISTS tdm;
DROP TABLE IF EXISTS tdm2;
CREATE TABLE tdm (x DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY x SETTINGS write_final_mark = 0;
INSERT INTO tdm VALUES (now());
SELECT count(x) FROM tdm WHERE toDate(x) < toDate(now(), 'Europe/Moscow') SETTINGS max_rows_to_read = 1;

SELECT toDate(-1), toDate(10000000000000, 'Europe/Moscow'), toDate(100), toDate(65536, 'UTC'), toDate(65535, 'Europe/Moscow');
SELECT toDateTime(-1, 'Europe/Moscow'), toDateTime(10000000000000, 'Europe/Moscow'), toDateTime(1000, 'Europe/Moscow');

CREATE TABLE tdm2 (timestamp UInt32) ENGINE = MergeTree ORDER BY timestamp SETTINGS index_granularity = 1;

INSERT INTO tdm2 VALUES (toUnixTimestamp('2000-01-01 13:12:12')), (toUnixTimestamp('2000-01-01 14:12:12')), (toUnixTimestamp('2000-01-01 15:12:12'));

SET max_rows_to_read = 1;
SELECT toDateTime(timestamp) FROM tdm2 WHERE toHour(toDateTime(timestamp)) = 13;

DROP TABLE tdm;
DROP TABLE tdm2;
