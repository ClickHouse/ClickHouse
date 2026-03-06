DROP TABLE IF EXISTS fsst_test_table;
CREATE TABLE fsst_test_table (id UInt64, s String)
ENGINE = MergeTree() ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO fsst_test_table SELECT number, ('hello world') FROM numbers(2048);
INSERT INTO fsst_test_table SELECT number, ('') FROM numbers(2048);
INSERT INTO fsst_test_table SELECT number, ('тест на славянина') FROM numbers(2048);

SELECT sum(id) AS sum1 FROM fsst_test_table WHERE s = 'тест на славянина';
SELECT * FROM fsst_test_table WHERE id > 100 AND id < 200;

DROP TABLE fsst_test_table;