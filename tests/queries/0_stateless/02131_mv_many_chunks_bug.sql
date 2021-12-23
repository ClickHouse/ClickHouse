drop table if exists t;
drop table if exists t_mv;

create table t (x UInt64) engine = MergeTree order by x;
create materialized view t_mv engine = MergeTree order by tuple() as select uniq(x), bitAnd(x, 255) as y from t group by y;

set max_bytes_before_external_group_by = 1000000000;
set group_by_two_level_threshold = 100;
set min_insert_block_size_rows = 100;

insert into t select number from numbers(300);
select count() from (select y from t_mv group by y);

drop table if exists t;
drop table if exists t_mv;
