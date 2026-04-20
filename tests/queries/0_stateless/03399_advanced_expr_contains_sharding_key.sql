drop table if exists local_table;
drop table if exists distributed_table;
drop table if exists distributed_table2;

set optimize_skip_unused_shards = true;
set prefer_localhost_replica=0;
SET allow_experimental_analyzer = 1;

create table local_table(id UInt64) engine MergeTree order by id;
create table distributed_table as local_table engine Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table, id);
create table distributed_table2 as local_table engine Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), local_table);

insert into local_table select number from numbers(100);

select 'query plan of GROUP BY sharding key';
explain select count() from distributed_table group by id;
explain select count() from distributed_table2 group by id;

explain select count() from distributed_table group by toString(id);
explain select count() from distributed_table2 group by toString(id);

select 'query plan of DISTINCT sharding key';
explain select distinct id from distributed_table;
explain select distinct id from distributed_table2;

explain select distinct toString(id) from distributed_table;
explain select distinct toString(id) from distributed_table2;

select 'query plan of LIMIT BY sharding key';
explain select * from distributed_table limit 1 by id;
explain select * from distributed_table2 limit 1 by id;

explain select * from distributed_table limit 1 by toString(id);
explain select * from distributed_table2 limit 1 by toString(id);

drop table if exists local_table;
drop table if exists distributed_table;
drop table if exists distributed_table2;