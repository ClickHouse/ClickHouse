-- Tags: no-parallel

SELECT '-- test FORMAT clause --';
SET output_format_write_statistics = 0;
SELECT number, 'Hello & world' FROM numbers(3) FORMAT Tsv;
SELECT number, 'Hello & world' FROM numbers(3) FORMAT csv;
SELECT number, 'Hello & world' FROM numbers(3) FORMAT xMl;
SELECT number, 'Hello & world' FROM numbers(3) FORMAT JsonStrINGs;
SELECT number, 'Hello & world' FROM numbers(3) FORMAT VERTICAL;

SELECT '-- test table function --';
INSERT INTO FUNCTION file('data_00309_formats_case_insensitive', 'Csv') SELECT number, 'Hello & world' FROM numbers(3) SETTINGS engine_file_truncate_on_insert=1;
SELECT * FROM file('data_00309_formats_case_insensitive', 'Csv');

INSERT INTO FUNCTION file('data_00309_formats_case_insensitive.cSv') SELECT number, 'Hello & world' FROM numbers(3) SETTINGS engine_file_truncate_on_insert=1;
SELECT * FROM file('data_00309_formats_case_insensitive.cSv');

SELECT '-- test other function --';
SELECT * FROM format(cSv, '0,Hello & world');

SELECT '-- test table engine --';
DROP TABLE IF EXISTS test_00309_formats_case_insensitive;
CREATE TABLE test_00309_formats_case_insensitive(a Int64, b String) ENGINE=File(Csv);
INSERT INTO test_00309_formats_case_insensitive SELECT number, 'Hello & world' FROM numbers(3);
SELECT * FROM test_00309_formats_case_insensitive;
