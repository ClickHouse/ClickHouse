SELECT * from numbers(1);
select * from format(JSONEachRow, '{"x" : [123, "str"]}');
SELECT * from numbers(1) SETTINGS readonly=1;
select * from format(JSONEachRow, '{"x" : [123, "str"]}') SETTINGS readonly=1; -- { serverError READONLY }
set readonly=0;