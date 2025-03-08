insert into function null() select * from input('x Int, y String') settings async_insert=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

create table x (x Int, y String) engine=Memory;
insert into x select * from input('x Int, y String') settings async_insert=1 format JSONEachRow {"x" : 1, "y" : "string1"}, {"y" : "string2", "x" : 2};

select * from x;
