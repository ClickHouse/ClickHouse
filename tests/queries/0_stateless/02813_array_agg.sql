-- Disable force_primary_key_reverse_order: reversed key reverses physical row order changing array_agg output
SET force_primary_key_reverse_order = 0;
drop table if exists t;
create table t (n Int32, s String) engine=MergeTree order by n;

insert into t select number, 'hello, world!' from numbers (5);

select array_agg(s) from t;

select aRray_Agg(s) from t group by n;

drop table t;
