drop table if exists m;

create table m (a int) engine Log;

insert into m values (1);

set enable_analyzer = true, optimize_rewrite_sum_if_to_count_if=1;

EXPLAIN QUERY TREE select sum(multiIf(a = 1, 1, 0)) from m;

drop table m;
