-- Tags: no-fasttest

set input_format_try_infer_integers=1;
set input_format_try_infer_exponent_floats=1;

select 'JSONEachRow';
desc format(JSONEachRow, '{"x" : 123}');
desc format(JSONEachRow, '{"x" : [123, 123]}');
desc format(JSONEachRow, '{"x" : {"a" : [123, 123]}}');
desc format(JSONEachRow, '{"x" : {"a" : [123, 123]}}\n{"x" : {"b" : [321, 321]}}');
desc format(JSONEachRow, '{"x" : 123}\n{"x" : 123.123}');
desc format(JSONEachRow, '{"x" : 123}\n{"x" : 1e2}');
desc format(JSONEachRow, '{"x" : [123, 123]}\n{"x" : [321.321, 312]}');
desc format(JSONEachRow, '{"x" : {"a" : [123, 123]}}\n{"x" : {"b" : [321.321, 123]}}');

select 'CSV';
desc format(CSV, '123');
desc format(CSV, '"[123, 123]"');
desc format(CSV, '"{\'a\' : [123, 123]}"');
desc format(CSV, '"{\'a\' : [123, 123]}"\n"{\'b\' : [321, 321]}"');
desc format(CSV, '123\n123.123');
desc format(CSV, '122\n1e2');
desc format(CSV, '"[123, 123]"\n"[321.321, 312]"');
desc format(CSV, '"{\'a\' : [123, 123]}"\n"{\'b\' : [321.321, 123]}"');

select 'TSV';
desc format(TSV, '123');
desc format(TSV, '[123, 123]');
desc format(TSV, '{\'a\' : [123, 123]}');
desc format(TSV, '{\'a\' : [123, 123]}\n{\'b\' : [321, 321]}');
desc format(TSV, '123\n123.123');
desc format(TSV, '122\n1e2');
desc format(TSV, '[123, 123]\n[321.321, 312]');
desc format(TSV, '{\'a\' : [123, 123]}\n{\'b\' : [321.321, 123]}');

select 'Values';
desc format(Values, '(123)');
desc format(Values, '([123, 123])');
desc format(Values, '({\'a\' : [123, 123]})');
desc format(Values, '({\'a\' : [123, 123]}), ({\'b\' : [321, 321]})');
desc format(Values, '(123), (123.123)');
desc format(Values, '(122), (1e2)');
desc format(Values, '([123, 123])\n([321.321, 312])');
desc format(Values, '({\'a\' : [123, 123]}), ({\'b\' : [321.321, 123]})');


