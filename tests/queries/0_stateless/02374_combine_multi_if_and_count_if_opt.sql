drop table if exists m;

create table m (a int) engine Log;

insert into m values (1);

explain syntax select sum(multiIf(a = 1, 1, 0)) from m settings optimize_rewrite_sum_if_to_count_if=1;

drop table m;
