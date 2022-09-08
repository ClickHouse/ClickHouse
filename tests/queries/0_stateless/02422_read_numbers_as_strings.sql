select * from format(JSONEachRow, '{"x" : 123}\n{"x" : "str"}');
select * from format(JSONEachRow, '{"x" : [123, "str"]}');
select * from format(JSONEachRow, '{"x" : [123, "456"]}\n{"x" : ["str", "rts"]}');
