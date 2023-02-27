-- Tags: no-fasttest

select * from format(JSONEachRow, '{"a" : {}}, {"a" : {"b" : 1}}')
