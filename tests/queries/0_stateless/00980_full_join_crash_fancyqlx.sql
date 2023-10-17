drop table if exists test_join;

create table test_join (date Date, id Int32, name Nullable(String)) engine = MergeTree partition by date order by id;

insert into test_join values ('2019-01-01', 1, 'a');
insert into test_join values ('2019-01-01', 2, 'b');
insert into test_join values ('2019-01-01', 3, 'c');
insert into test_join values ('2019-01-01', 1, null);

SELECT id, date, name FROM (SELECT id, date, name FROM test_join GROUP BY id, name, date) js1
FULL OUTER JOIN (SELECT id, date, name FROM test_join GROUP BY id, name, date) js2
USING (id, name, date)
ORDER BY id, name;

drop table test_join;
