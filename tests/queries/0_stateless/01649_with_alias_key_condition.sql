drop table if exists alias_key_condition;

create table alias_key_condition ( i int, j int ) engine MergeTree order by i;

insert into alias_key_condition values (1, 2), (3, 4);

set force_primary_key = 1;

with i as k select * from alias_key_condition where k = (select i from alias_key_condition where i = 3);

drop table if exists alias_key_condition;
