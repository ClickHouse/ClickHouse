-- Tags: long, no-parallel, no-msan, no-tsan, no-asan
-- set no-parallel and no sanitizers tag is to prevent timeout of this test

drop table if exists t;

create table t (c1 Int64, c2 String, c3 DateTime, c4 Int8, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String, c11 String, c12 String, c13 Int8, c14 Int64, c15 String, c16 String, c17 String, c18 Int64, c19 Int64, c20 Int64) engine MergeTree order by c18 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

insert into t (c1, c18) select number, -number from numbers(2000000);

alter table t add projection p_norm (select * order by c1);

optimize table t final;

alter table t materialize projection p_norm settings mutations_sync = 1;

set optimize_use_projections = 1, max_rows_to_read = 3;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

select c18 from t where c1 < 0;

drop table t;
