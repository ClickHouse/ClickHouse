-- Tags: no-fasttest
set allow_experimental_object_type=1;
desc format(JSONEachRow, '{"a" : {"b" : {"c" : 1, "d" : "str"}}}');
set allow_experimental_object_type=0;
desc format(JSONEachRow, '{"a" : {"b" : {"c" : 1, "d" : "str"}}}'); -- {serverError 652}

