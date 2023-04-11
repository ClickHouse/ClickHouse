DROP TABLE IF EXISTS tdm__fuzz_23;
CREATE TABLE tdm__fuzz_23 (`x` UInt256) ENGINE = MergeTree ORDER BY x SETTINGS write_final_mark = 0;
INSERT INTO tdm__fuzz_23 FORMAT Values (1);
SELECT count(x) FROM tdm__fuzz_23 WHERE toDate(x) < toDate(now(), 'Asia/Istanbul') SETTINGS max_rows_to_read = 1;
DROP TABLE tdm__fuzz_23;
