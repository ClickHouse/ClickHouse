set allow_experimental_object_type = 1;

select * from format(JSONAsString, 't DateTime default ''2020-01-01'', json String, x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}');
select * from format(JSONAsString, 't DateTime default ''2020-01-01'', json1 String, json2 String, x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}');
select * from format(JSONAsString, 't DateTime default ''2020-01-01'', x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}'); -- { serverError BAD_ARGUMENTS }

select * from format(JSONAsObject, 't DateTime default ''2020-01-01'', json Object(''json''), x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}');
select * from format(JSONAsObject, 't DateTime default ''2020-01-01'', json1  Object(''json''), json2  Object(''json''), x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}');
select * from format(JSONAsObject, 't DateTime default ''2020-01-01'', x UInt32', '{"a" : 1}, {"a" : 2}, {"a" : 3}, {"a" : 4}, {"a" : 5}'); -- { serverError BAD_ARGUMENTS }

