-- Tags: no-replicated-database, no-parallel

INSERT INTO TABLE FUNCTION file('02771/dir1/subdir11/data1.csv', 'CSV', 's String') SELECT 'This is file data1' SETTINGS engine_file_truncate_on_insert=1;
INSERT INTO TABLE FUNCTION file('02771/dir2/subdir22/data2.csv', 'CSV', 's String') SELECT 'This is file data2' SETTINGS engine_file_truncate_on_insert=1;

SELECT *, _file FROM file('02771/dir{?/subdir?1/da,2/subdir2?/da}ta1.csv', CSV);
SELECT *, _file FROM file('02771/dir{?/subdir?1/da,2/subdir2?/da}ta2.csv', CSV);
SELECT *, _file FROM file('02771/dir?/{subdir?1/data1,subdir2?/data2}.csv', CSV);
