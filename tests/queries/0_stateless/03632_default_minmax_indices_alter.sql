-- Test for issue #75677

drop table if exists t;

create table t (a UInt64, s String) engine = MergeTree order by tuple() settings add_minmax_index_for_numeric_columns = 1;

show create table t;

alter table t drop column s;

show create table t;

drop table t;