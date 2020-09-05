DROP TABLE IF EXISTS binary_op_mono;

CREATE TABLE binary_op_mono(i int, j int) ENGINE MergeTree PARTITION BY toDate(i / 1000) ORDER BY j;

INSERT INTO binary_op_mono VALUES (toUnixTimestamp('2020-09-01 00:00:00') * 1000, 1), (toUnixTimestamp('2020-09-01 00:00:00') * 1000, 2);

SET max_rows_to_read = 1;
SELECT * FROM binary_op_mono WHERE toDate(i / 1000) = '2020-09-02';

DROP TABLE IF EXISTS binary_op_mono;
