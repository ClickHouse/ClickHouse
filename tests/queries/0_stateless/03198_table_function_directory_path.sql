-- Tags: no-parallel

INSERT INTO FUNCTION file('data_03198_table_function_directory_path/1.csv', 'csv') SELECT '1.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/2.csv', 'csv') SELECT '2.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir/3.csv', 'csv') SELECT '3.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir1/dir/4.csv', 'csv') SELECT '4.csv' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO FUNCTION file('data_03198_table_function_directory_path/dir2/dir/5.csv', 'csv') SELECT '5.csv' SETTINGS engine_file_truncate_on_insert=1;

SELECT COUNT(*) FROM file('data_03198_table_function_directory_path');
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/');
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/dir');
SELECT COUNT(*) FROM file('data_03198_table_function_directory_path/*/dir', 'csv'); -- { serverError 74, 636 }
SELECT COUNT(*) FROM file('data_03198_table_function_directory_pat'); -- { serverError 400 }
