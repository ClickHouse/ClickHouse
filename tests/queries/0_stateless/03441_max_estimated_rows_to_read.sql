-- Database
DROP DATABASE IF EXISTS max_estimated_rows_to_read_example;
CREATE DATABASE max_estimated_rows_to_read_example;
USE max_estimated_rows_to_read_example;

-- Tables
CREATE TABLE company(
    id UInt64 NOT NULL PRIMARY KEY
);

INSERT INTO company (id) VALUES
    (1),
    (2),
    (3),
    (4),
    (5)
;

SELECT * FROM company;
SELECT * FROM company SETTINGS max_estimated_rows_to_read=4; -- { serverError TOO_MANY_ROWS }
SELECT * FROM company SETTINGS max_estimated_rows_to_read=5;
