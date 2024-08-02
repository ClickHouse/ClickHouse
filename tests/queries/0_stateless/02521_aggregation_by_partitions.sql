-- Tags: long, no-object-storage

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

set max_threads = 16;
set allow_aggregate_partitions_independently = 1;
set force_aggregate_partitions_independently = 1;
set optimize_use_projections = 0;
set optimize_trivial_insert_select = 1;

set allow_prefetched_read_pool_for_remote_filesystem = 0;
set allow_prefetched_read_pool_for_local_filesystem = 0;

create table t1(a UInt32) engine=MergeTree order by tuple() partition by a % 4 settings index_granularity = 8192, index_granularity_bytes = 10485760;

system stop merges t1;

insert into t1 select number from numbers_mt(1e6);
insert into t1 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t1 group by a;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t1 group by a);

drop table t1;

create table t2(a UInt32) engine=MergeTree order by tuple() partition by a % 8 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t2;

insert into t2 select number from numbers_mt(1e6);
insert into t2 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t2 group by a;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t2 group by a);

drop table t2;

create table t3(a UInt32) engine=MergeTree order by tuple() partition by a % 16 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t3;

insert into t3 select number from numbers_mt(1e6);
insert into t3 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t3 group by a;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t3 group by a);

select throwIf(count() != 4) from remote('127.0.0.{1,2}', currentDatabase(), t3) group by a format Null;

-- if we happened to switch to external aggregation at some point, merging will happen as usual
select count() from (select throwIf(count() != 2) from t3 group by a) settings max_bytes_before_external_group_by = '1Ki';

drop table t3;

-- aggregation in order --

set optimize_aggregation_in_order = 1;

create table t4(a UInt32) engine=MergeTree order by a partition by a % 4 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t4;

insert into t4 select number from numbers_mt(1e6);
insert into t4 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t4 group by a settings read_in_order_two_level_merge_threshold = 1e12;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t4 group by a);

drop table t4;

create table t5(a UInt32) engine=MergeTree order by a partition by a % 8 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t5;

insert into t5 select number from numbers_mt(1e6);
insert into t5 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t5 group by a settings read_in_order_two_level_merge_threshold = 1e12;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t5 group by a);

drop table t5;

create table t6(a UInt32) engine=MergeTree order by a partition by a % 16 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t6;

insert into t6 select number from numbers_mt(1e6);
insert into t6 select number from numbers_mt(1e6);

-- { echoOn }
explain pipeline select a from t6 group by a settings read_in_order_two_level_merge_threshold = 1e12;
-- { echoOff }

select count() from (select throwIf(count() != 2) from t6 group by a);

drop table t6;

set optimize_aggregation_in_order = 0;

create table t7(a UInt32) engine=MergeTree order by a partition by intDiv(a, 2) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t7 select number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 2) as a1 from t7 group by a1
) where explain like '%Skip merging: %';

drop table t7;

create table t8(a UInt32) engine=MergeTree order by a partition by intDiv(a, 2) * 2 + 1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t8 select number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 2) + 1 as a1 from t8 group by a1
) where explain like '%Skip merging: %';

drop table t8;

create table t9(a UInt32) engine=MergeTree order by a partition by intDiv(a, 2) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t9 select number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 3) as a1 from t9 group by a1
) where explain like '%Skip merging: %';

drop table t9;

create table t10(a UInt32, b UInt32) engine=MergeTree order by a partition by (intDiv(a, 2), intDiv(b, 3)) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t10 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 2) + 1 as a1, intDiv(b, 3) as b1 from t10 group by a1, b1, pi()
) where explain like '%Skip merging: %';

drop table t10;

-- multiplication by 2 is not injective, so optimization is not applicable
create table t11(a UInt32, b UInt32) engine=MergeTree order by a partition by (intDiv(a, 2), intDiv(b, 3)) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t11 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 2) + 1 as a1, intDiv(b, 3) * 2 as b1 from t11 group by a1, b1, pi()
) where explain like '%Skip merging: %';

drop table t11;

create table t12(a UInt32, b UInt32) engine=MergeTree order by a partition by a % 16;

insert into t12 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a, b from t12 group by a, b, pi()
) where explain like '%Skip merging: %';

drop table t12;

create table t13(a UInt32, b UInt32) engine=MergeTree order by a partition by (intDiv(a, 2), intDiv(b, 3)) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t13 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select s from t13 group by intDiv(a, 2) + intDiv(b, 3) as s, pi()
) where explain like '%Skip merging: %';

drop table t13;

create table t14(a UInt32, b UInt32) engine=MergeTree order by a partition by intDiv(a, 2) + intDiv(b, 3) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t14 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select intDiv(a, 2) as a1, intDiv(b, 3) as b1 from t14 group by a1, b1, pi()
) where explain like '%Skip merging: %';

drop table t14;

-- to few partitions --
create table t15(a UInt32, b UInt32) engine=MergeTree order by a partition by a < 90 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t15 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a from t15 group by a
) where explain like '%Skip merging: %'
settings force_aggregate_partitions_independently = 0;

drop table t15;

-- to many partitions --
create table t16(a UInt32, b UInt32) engine=MergeTree order by a partition by a % 16 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t16 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a from t16 group by a
) where explain like '%Skip merging: %'
settings force_aggregate_partitions_independently = 0, max_number_of_partitions_for_independent_aggregation = 4;

drop table t16;

-- to big skew --
create table t17(a UInt32, b UInt32) engine=MergeTree order by a partition by a < 90 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t17 select number, number from numbers_mt(100);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a from t17 group by a
) where explain like '%Skip merging: %'
settings force_aggregate_partitions_independently = 0, max_threads = 4;

drop table t17;

create table t18(a UInt32, b UInt32) engine=MergeTree order by a partition by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t18 select number, number from numbers_mt(50);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a1 from t18 group by intDiv(a, 2) as a1
) where explain like '%Skip merging: %';

drop table t18;

create table t19(a UInt32, b UInt32) engine=MergeTree order by a partition by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t19 select number, number from numbers_mt(50);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a1 from t19 group by blockNumber() as a1
) where explain like '%Skip merging: %';

drop table t19;

create table t20(a UInt32, b UInt32) engine=MergeTree order by a partition by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t20 select number, number from numbers_mt(50);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a1 from t20 group by rand(a) as a1
) where explain like '%Skip merging: %';

drop table t20;

create table t21(a UInt64, b UInt64) engine=MergeTree order by a partition by a % 16 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t21 select number, number from numbers_mt(1e6);

select a from t21 group by a limit 10 format Null;

drop table t21;

create table t22(a UInt32, b UInt32) engine=SummingMergeTree order by a partition by a % 16 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t22 select number, number from numbers_mt(1e6);

select replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') from (
    explain actions=1 select a from t22 final group by a
) where explain like '%Skip merging: %';

drop table t22;
