drop table if exists test1;
create table kv (key int, v1 int, v2 int, v3 int, v4 int, v5 int) engine MergeTree order by key;

insert into kv values (1, 10, 20, 10, 20, 30), (2, 11, 20, 10, 20, 30), (1, 18, 20, 10, 20, 30), (1, 20, 20, 10, 20, 30), (3, 70, 20, 10, 20, 30), (4, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (5, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (8, 30, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (6, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (7, 18, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (7, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30), (8, 10, 20, 10, 20, 30), (1, 10, 20, 10, 20, 30);

select key, untuple(argMax((* except (key),), v1), 'max_') from kv group by key order by max_v3 format TSVWithNames;

with untuple((* apply (avg, 'avg_'),)) select avg_v2 + avg_v4 from kv;

drop table if exists test1;
