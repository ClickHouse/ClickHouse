INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_1.csv', 'CSV', 'x UInt64')
SELECT 1
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_2.csv', 'CSV', 'x UInt64')
SELECT 2
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_mixed_concrete.csv', 'CSV', 'x UInt64')
SELECT 10
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_mixed_glob_1.csv', 'CSV', 'x UInt64')
SELECT 11
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_mixed_glob_2.csv', 'CSV', 'x UInt64')
SELECT 12
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_csv_extension.csv', 'CSV', 'x UInt64')
SELECT 20
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_data_extension.data', 'CSV', 'x UInt64')
SELECT 21
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_different_format.csv', 'CSV', 'x UInt64, y UInt64')
SELECT 30, 300
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_file_array_arg_different_format.tsv', 'TSV', 'x UInt64, y UInt64')
SELECT 31, 310
SETTINGS engine_file_truncate_on_insert = 1;

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_1.csv', currentDatabase() || '_file_array_arg_2.csv'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file(arrayConcat([currentDatabase() || '_file_array_arg_1.csv'], [currentDatabase() || '_file_array_arg_2.csv']), auto, 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_{1,2}.csv'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_mixed_concrete.csv', currentDatabase() || '_file_array_arg_mixed_glob_*.csv'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT groupArray(x)
FROM
(
    SELECT x
    FROM file([currentDatabase() || '_file_array_arg_csv_extension.csv', currentDatabase() || '_file_array_arg_data_extension.data'], 'CSV', 'x UInt64')
    ORDER BY x
);

SELECT sum(x + y)
FROM file([currentDatabase() || '_file_array_arg_different_format.csv', currentDatabase() || '_file_array_arg_different_format.tsv'], auto, 'x UInt64, y UInt64'); -- { serverError INCORRECT_DATA, CANNOT_READ_ALL_DATA, NUMBER_OF_COLUMNS_DOESNT_MATCH, CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT * FROM file([concat(toString(rand()), '.csv')], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM file([], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM file([1, 2], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

SELECT * FROM file([currentDatabase() || '_file_array_arg_archive.zip::data.csv'], 'CSV', 'x UInt64'); -- { serverError BAD_ARGUMENTS }

INSERT INTO FUNCTION file([currentDatabase() || '_file_array_arg_insert_single.csv'], 'CSV', 'x UInt64')
SELECT 1; -- { serverError DATABASE_ACCESS_DENIED }

INSERT INTO FUNCTION file([currentDatabase() || '_file_array_arg_insert_1.csv', currentDatabase() || '_file_array_arg_insert_2.csv'], 'CSV', 'x UInt64')
SELECT 1; -- { serverError DATABASE_ACCESS_DENIED }
