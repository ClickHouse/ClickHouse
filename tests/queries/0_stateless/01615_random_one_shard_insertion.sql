drop table if exists shard;
drop table if exists distr;

create table shard (id Int32) engine = MergeTree order by cityHash64(id);
create table distr as shard engine Distributed (test_cluster_two_shards_localhost, currentDatabase(), shard);

insert into distr (id) values (0), (1);  -- { serverError 55; }

set insert_distributed_sync = 1;

insert into distr (id) values (0), (1);  -- { serverError 55; }

set insert_distributed_sync = 0;
set insert_distributed_one_random_shard = 1;

insert into distr (id) values (0), (1);
insert into distr (id) values (2), (3);

select * from distr order by id;

drop table if exists shard;
drop table if exists distr;
