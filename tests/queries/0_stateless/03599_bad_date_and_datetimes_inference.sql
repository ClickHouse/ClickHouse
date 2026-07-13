set date_time_input_format='basic';
set session_timezone='UTC';

select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31 23:59:59"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31 23:59:59", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2149-06-07"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2149-06-07", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2106-02-07 06:28:16"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2106-02-07 06:28:16", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1899-12-31 23:59:59"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1899-12-31 23:59:59", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2300-01-01 00:00:00.000000000"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2300-01-01 00:00:00.000000000", "s" : "some string"}');

select '----------------------------------------------';

set date_time_input_format='best_effort';
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1800-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31 23:59:59"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1969-12-31 23:59:59", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2149-06-07"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2149-06-07", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "3000-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2106-02-07 06:28:16"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2106-02-07 06:28:16", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01 00:00:00"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1900-01-01 00:00:00", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1899-12-31 23:59:59"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "1899-12-31 23:59:59", "s" : "some string"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2300-01-01 00:00:00.000000000"}');
select d, toTypeName(d) from format(JSONEachRow, '{"d" : "2300-01-01 00:00:00.000000000", "s" : "some string"}');



