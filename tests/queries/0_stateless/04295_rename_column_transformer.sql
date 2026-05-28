SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS rename_column_transformer;

CREATE TABLE rename_column_transformer
(
    id UInt8,
    value UInt8,
    value_2 UInt8,
    time UInt8
)
ENGINE = Memory;

INSERT INTO rename_column_transformer VALUES (1, 10, 20, 5), (2, 11, 21, 6);

SELECT 'explicit single';
DESCRIBE (SELECT * RENAME value AS value_diff_sum FROM rename_column_transformer);
SELECT * RENAME value AS value_diff_sum FROM rename_column_transformer ORDER BY id;

SELECT 'explicit list';
DESCRIBE (SELECT * RENAME (value AS value_diff_sum, value_2 AS value_2_diff_sum) FROM rename_column_transformer);
SELECT * RENAME (value AS value_diff_sum, value_2 AS value_2_diff_sum) FROM rename_column_transformer ORDER BY id;

SELECT 'replace and rename';
DESCRIBE (SELECT * REPLACE (value + 10 AS value) RENAME value AS value_plus_ten FROM rename_column_transformer);
SELECT * REPLACE (value + 10 AS value) RENAME value AS value_plus_ten FROM rename_column_transformer ORDER BY id;

SELECT 'lambda rename after apply';
DESCRIBE (SELECT * EXCEPT time APPLY toString RENAME (col -> concat(col, '_str')) FROM rename_column_transformer);
SELECT * EXCEPT time APPLY toString RENAME (col -> concat(col, '_str')) FROM rename_column_transformer ORDER BY id;

SELECT 'lambda rename after chained apply';
DESCRIBE (SELECT * EXCEPT time APPLY (x -> x + 1) APPLY toString RENAME (col -> col || '_plus_one_str') FROM rename_column_transformer);
SELECT * EXCEPT time APPLY (x -> x + 1) APPLY toString RENAME (col -> col || '_plus_one_str') FROM rename_column_transformer ORDER BY id;

SELECT * EXCEPT value RENAME value AS missing FROM rename_column_transformer; -- { serverError BAD_ARGUMENTS }
SELECT * RENAME (col -> 1) FROM rename_column_transformer; -- { serverError BAD_ARGUMENTS }
SELECT * RENAME value AS renamed APPLY toString FROM rename_column_transformer; -- { clientError SYNTAX_ERROR }

DROP TABLE rename_column_transformer;
