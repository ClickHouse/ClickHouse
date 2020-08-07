DROP TABLE IF EXISTS tdm;
CREATE TABLE tdm (x DateTime) ENGINE = MergeTree ORDER BY x SETTINGS write_final_mark = 0;
INSERT INTO tdm VALUES (now());
SELECT count(x) FROM tdm WHERE toDate(x) < today() SETTINGS max_rows_to_read = 1;

SELECT toDate(-1), toDate(10000000000000), toDate(100), toDate(65536), toDate(65535);
SELECT toDateTime(-1), toDateTime(10000000000000), toDateTime(1000);

DROP TABLE tdm;
