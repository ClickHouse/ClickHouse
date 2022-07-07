-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

create table local_t_l5ydey on cluster test_shard_localhost (
    c_qv5rv INTEGER ,
    c_rutjs4 INTEGER ,
    c_wmj INTEGER ,
    c_m3 TEXT NOT NULL,
    primary key(c_qv5rv)
) engine=ReplicatedMergeTree('/clickhouse/tables/test_{database}/{shard}/local_t_l5ydey', '{replica}');

create table t_l5ydey on cluster test_shard_localhost as local_t_l5ydey
    engine=Distributed('test_shard_localhost', currentDatabase(),'local_t_l5ydey', rand());

insert into local_t_l5ydey values (1, 2, 3, '4');
insert into t_l5ydey values (5, 6, 7, '8');
system flush distributed t_l5ydey;

select * from t_l5ydey order by c_qv5rv;
show create t_l5ydey;
