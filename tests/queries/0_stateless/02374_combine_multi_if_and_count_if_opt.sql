drop table if exists m;

create table m (a int) engine Log;

insert into m values (1);

set optimize_rewrite_sum_if_to_count_if=1;

explain syntax select sum(multiIf(a = 1, 1, 0)) from m;

set optimize_rewrite_sum_if_to_count_if=0;

drop table m;
