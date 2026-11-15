drop table if exists t;

CREATE TABLE t (id UInt64, id2 UInt64, id3 UInt64, PROJECTION t_reverse (SELECT id, id2, id3 ORDER BY id2, id, id3)) ENGINE = MergeTree ORDER BY (id) settings index_granularity = 4;

insert into t SELECT number, -number, number FROM numbers(10000);

set max_rows_to_read = 4;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;
select count() from t where id = 3;

drop table t;
