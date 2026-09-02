-- Tags: no-fasttest

set input_format_try_infer_dates=1;
set input_format_try_infer_datetimes=1;

select 'JSONEachRow';
desc format(JSONEachRow, '{"x" : "2020-01-01"}');
desc format(JSONEachRow, '{"x" : "2020-01-01 00:00:00.00000"}');
desc format(JSONEachRow, '{"x" : "2020-01-01 00:00:00"}');
desc format(JSONEachRow, '{"x" : ["2020-01-01", "2020-01-02"]}');
desc format(JSONEachRow, '{"x" : ["2020-01-01", "2020-01-01 00:00:00"]}');
desc format(JSONEachRow, '{"x" : ["2020-01-01 00:00:00", "2020-01-01 00:00:00"]}');
desc format(JSONEachRow, '{"x" : {"date1" : "2020-01-01 00:00:00", "date2" : "2020-01-01"}}');
desc format(JSONEachRow, '{"x" : ["2020-01-01 00:00:00", "2020-01-01"]}\n{"x" : ["2020-01-01"]}');
desc format(JSONEachRow, '{"x" : ["2020-01-01 00:00:00"]}\n{"x" : ["2020-01-01"]}');
desc format(JSONEachRow, '{"x" : "2020-01-01 00:00:00"}\n{"x" : "2020-01-01"}');
desc format(JSONEachRow, '{"x" : ["2020-01-01 00:00:00", "Some string"]}');
desc format(JSONEachRow, '{"x" : "2020-01-01 00:00:00"}\n{"x" : "Some string"}');
desc format(JSONEachRow, '{"x" : ["2020-01-01 00:00:00", "2020-01-01"]}\n{"x" : ["2020-01-01", "Some string"]}');
desc format(JSONEachRow, '{"x" : {"key1" : [["2020-01-01 00:00:00"]], "key2" : [["2020-01-01"]]}}\n{"x" : {"key1" : [["2020-01-01"]], "key2" : [["Some string"]]}}');

select 'CSV';
desc format(CSV, '"2020-01-01"');
desc format(CSV, '"2020-01-01 00:00:00.00000"');
desc format(CSV, '"2020-01-01 00:00:00"');
desc format(CSV, '"[\'2020-01-01\', \'2020-01-02\']"');
desc format(CSV, '"[\'2020-01-01\', \'2020-01-01 00:00:00\']"');
desc format(CSV, '"[\'2020-01-01 00:00:00\', \'2020-01-01 00:00:00\']"');
desc format(CSV, '"{\'date1\' : \'2020-01-01 00:00:00\', \'date2\' : \'2020-01-01\'}"');
desc format(CSV, '"[\'2020-01-01 00:00:00\', \'2020-01-01\']"\n"[\'2020-01-01\']"');
desc format(CSV, '"[\'2020-01-01 00:00:00\']"\n"[\'2020-01-01\']"');
desc format(CSV, '"2020-01-01 00:00:00"\n"2020-01-01"');
desc format(CSV, '"[\'2020-01-01 00:00:00\', \'Some string\']"');
desc format(CSV, '"2020-01-01 00:00:00"\n"Some string"');
desc format(CSV, '"[\'2020-01-01 00:00:00\', \'2020-01-01\']"\n"[\'2020-01-01\', \'Some string\']"');
desc format(CSV, '"{\'key1\' : [[\'2020-01-01 00:00:00\']], \'key2\' : [[\'2020-01-01\']]}"\n"{\'key1\' : [[\'2020-01-01\']], \'key2\' : [[\'Some string\']]}"');

select 'TSV';
desc format(TSV, '2020-01-01');
desc format(TSV, '2020-01-01 00:00:00.00000');
desc format(TSV, '2020-01-01 00:00:00');
desc format(TSV, '[\'2020-01-01\', \'2020-01-02\']');
desc format(TSV, '[\'2020-01-01\', \'2020-01-01 00:00:00\']');
desc format(TSV, '[\'2020-01-01 00:00:00\', \'2020-01-01 00:00:00\']');
desc format(TSV, '{\'date1\' : \'2020-01-01 00:00:00\', \'date2\' : \'2020-01-01\'}');
desc format(TSV, '[\'2020-01-01 00:00:00\', \'2020-01-01\']\n[\'2020-01-01\']');
desc format(TSV, '[\'2020-01-01 00:00:00\']\n[\'2020-01-01\']');
desc format(TSV, '2020-01-01 00:00:00\n2020-01-01');
desc format(TSV, '[\'2020-01-01 00:00:00\', \'Some string\']');
desc format(TSV, '2020-01-01 00:00:00\nSome string');
desc format(TSV, '[\'2020-01-01 00:00:00\', \'2020-01-01\']\n[\'2020-01-01\', \'Some string\']');
desc format(TSV, '{\'key1\' : [[\'2020-01-01 00:00:00\']], \'key2\' : [[\'2020-01-01\']]}\n{\'key1\' : [[\'2020-01-01\']], \'key2\' : [[\'Some string\']]}');

select 'Values';
desc format(Values, '(\'2020-01-01\')');
desc format(Values, '(\'2020-01-01 00:00:00.00000\')');
desc format(Values, '(\'2020-01-01 00:00:00\')');
desc format(Values, '([\'2020-01-01\', \'2020-01-02\'])');
desc format(Values, '([\'2020-01-01\', \'2020-01-01 00:00:00\'])');
desc format(Values, '([\'2020-01-01 00:00:00\', \'2020-01-01 00:00:00\'])');
desc format(Values, '({\'date1\' : \'2020-01-01 00:00:00\', \'date2\' : \'2020-01-01\'})');
desc format(Values, '([\'2020-01-01 00:00:00\', \'2020-01-01\'])\n([\'2020-01-01\'])');
desc format(Values, '([\'2020-01-01 00:00:00\']), ([\'2020-01-01\'])');
desc format(Values, '(\'2020-01-01 00:00:00\')\n(\'2020-01-01\')');
desc format(Values, '([\'2020-01-01 00:00:00\', \'Some string\'])');
desc format(Values, '(\'2020-01-01 00:00:00\')\n(\'Some string\')');
desc format(Values, '([\'2020-01-01 00:00:00\', \'2020-01-01\'])\n([\'2020-01-01\', \'Some string\'])');
desc format(Values, '({\'key1\' : [[\'2020-01-01 00:00:00\']], \'key2\' : [[\'2020-01-01\']]})\n({\'key1\' : [[\'2020-01-01\']], \'key2\' : [[\'Some string\']]})');


