drop table if exists tp;

create table tp (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y settings min_rows_for_wide_part = 4, min_bytes_for_wide_part = 32;

insert into tp select number, number from numbers(3);
insert into tp select number, number from numbers(5);

check table tp settings check_query_single_value_result=0, max_threads=1;

drop table tp;

create table tp (p Date, k UInt64, v1 UInt64, v2 Int64, projection p1 ( select p, sum(k), sum(v1), sum(v2) group by p) ) engine = MergeTree partition by toYYYYMM(p) order by k settings min_bytes_for_wide_part = 0;

insert into tp (p, k, v1, v2) values ('2018-05-15', 1, 1000, 2000), ('2018-05-16', 2, 3000, 4000), ('2018-05-17', 3, 5000, 6000), ('2018-05-18', 4, 7000, 8000);

check table tp settings check_query_single_value_result=0, max_threads=1;

drop table tp;

drop table if exists tp;
create table tp (x int, projection p (select sum(x))) engine = MergeTree order by x settings min_rows_for_wide_part = 2, min_bytes_for_wide_part = 0;
insert into tp values (1), (2), (3), (4);
select part_type from system.parts where database = currentDatabase() and table = 'tp';
select part_type from system.projection_parts where database = currentDatabase() and table = 'tp';
check table tp settings check_query_single_value_result=0, max_threads=1;
drop table tp;
