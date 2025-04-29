-- Database
DROP DATABASE IF EXISTS max_estimated_rows_to_read_example;
CREATE DATABASE max_estimated_rows_to_read_example;
USE max_estimated_rows_to_read_example;

-- Tables
CREATE TABLE company(
    id UInt64 NOT NULL PRIMARY KEY
);

INSERT INTO company (id) VALUES
    (0),
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9)
;

SELECT count() FROM company;
SELECT count() FROM company SETTINGS max_estimated_rows_to_read=9; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM company SETTINGS max_estimated_rows_to_read=10;
