SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

select untuple((* except (b),)) from (select 1 a, 2 b, 3 c);
select 'hello', untuple((* except (b),)), 'world' from (select 1 a, 2 b, 3 c);
select argMax(untuple(x)) from (select (number, number + 1) as x from numbers(10));
select argMax(untuple(x)), min(x) from (select (number, number + 1) as x from numbers(10)) having tuple(untuple(min(x))).1 != 42;

drop table if exists kv;
create table kv (key int, v1 int, v2 int, v3 int, v4 int, v5 int) engine MergeTree order by key;
insert into kv values (1, 10, 20, 10, 20, 30), (2, 11, 20, 10, 20, 30), (1, 18, 20, 10, 20, 30), (1, 20, 20, 10, 20, 30), (3, 70, 20, 10, 20, 30), (4, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (5, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (8, 30, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (6, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (7, 18, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (7, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (8, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30);
select key, untuple(argMax((* except (key),), v1)) from kv group by key order by key format TSVWithNames;
drop table if exists kv;
