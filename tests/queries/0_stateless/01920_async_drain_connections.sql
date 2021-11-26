drop table if exists t;

create table t (number UInt64) engine = Distributed(test_cluster_two_shards, system, numbers);
select * from t where number = 0 limit 2 settings sleep_in_receive_cancel_ms = 10000, max_execution_time = 5;

drop table t;
