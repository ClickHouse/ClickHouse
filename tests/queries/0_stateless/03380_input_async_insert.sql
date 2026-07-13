---
--- Analyzer
---

insert into function null() select * from input('x Int, y String') settings async_insert=1, allow_experimental_analyzer=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

insert into function null('auto') select * from input('x Int, y String') settings async_insert=1, allow_experimental_analyzer=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

insert into function null('x Int, y String') select * from input('x Int, y String') settings async_insert=1, allow_experimental_analyzer=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

---
--- Non-analyzer - does not support INSERT INTO FUNCTION null('auto') SELECT FROM input()
---
insert into function null() select * from input('x Int, y String') settings async_insert=1, allow_experimental_analyzer=0 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2}; -- { serverError QUERY_IS_PROHIBITED }

insert into function null('x Int, y String') select * from input('x Int, y String') settings async_insert=1, allow_experimental_analyzer=0 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

drop table if exists x;

create table x (x Int, y String) engine=Memory;
insert into x select * from input('x Int, y String') settings async_insert=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

select * from x;
