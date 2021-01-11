DROP TABLE IF EXISTS binary_op_mono1;
DROP TABLE IF EXISTS binary_op_mono2;
DROP TABLE IF EXISTS binary_op_mono3;
DROP TABLE IF EXISTS binary_op_mono4;
DROP TABLE IF EXISTS binary_op_mono5;
DROP TABLE IF EXISTS binary_op_mono6;
DROP TABLE IF EXISTS binary_op_mono7;
DROP TABLE IF EXISTS binary_op_mono8;

CREATE TABLE binary_op_mono1(i int, j int) ENGINE MergeTree PARTITION BY toDate(i / 1000) ORDER BY j;
CREATE TABLE binary_op_mono2(i int, j int) ENGINE MergeTree PARTITION BY 1000 / i ORDER BY j;
CREATE TABLE binary_op_mono3(i int, j int) ENGINE MergeTree PARTITION BY i + 1000 ORDER BY j;
CREATE TABLE binary_op_mono4(i int, j int) ENGINE MergeTree PARTITION BY 1000 + i ORDER BY j;
CREATE TABLE binary_op_mono5(i int, j int) ENGINE MergeTree PARTITION BY i - 1000 ORDER BY j;
CREATE TABLE binary_op_mono6(i int, j int) ENGINE MergeTree PARTITION BY 1000 - i ORDER BY j;
CREATE TABLE binary_op_mono7(i int, j int) ENGINE MergeTree PARTITION BY i / 1000.0 ORDER BY j;
CREATE TABLE binary_op_mono8(i int, j int) ENGINE MergeTree PARTITION BY 1000.0 / i ORDER BY j;

INSERT INTO binary_op_mono1 VALUES (toUnixTimestamp('2020-09-01 00:00:00') * 1000, 1), (toUnixTimestamp('2020-09-01 00:00:00') * 1000, 2);
INSERT INTO binary_op_mono2 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono3 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono4 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono5 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono6 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono7 VALUES (1, 1), (10000, 2);
INSERT INTO binary_op_mono8 VALUES (1, 1), (10000, 2);

SET max_rows_to_read = 1;
SELECT count() FROM binary_op_mono1 WHERE toDate(i / 1000) = '2020-09-02';
SELECT count() FROM binary_op_mono2 WHERE 1000 / i = 100;
SELECT count() FROM binary_op_mono3 WHERE i + 1000 = 500;
SELECT count() FROM binary_op_mono4 WHERE 1000 + i = 500;
SELECT count() FROM binary_op_mono5 WHERE i - 1000 = 1234;
SELECT count() FROM binary_op_mono6 WHERE 1000 - i = 1234;
SELECT count() FROM binary_op_mono7 WHERE i / 1000.0 = 22.3;
SELECT count() FROM binary_op_mono8 WHERE 1000.0 / i = 33.4;

DROP TABLE IF EXISTS binary_op_mono1;
DROP TABLE IF EXISTS binary_op_mono2;
DROP TABLE IF EXISTS binary_op_mono3;
DROP TABLE IF EXISTS binary_op_mono4;
DROP TABLE IF EXISTS binary_op_mono5;
DROP TABLE IF EXISTS binary_op_mono6;
DROP TABLE IF EXISTS binary_op_mono7;
DROP TABLE IF EXISTS binary_op_mono8;
