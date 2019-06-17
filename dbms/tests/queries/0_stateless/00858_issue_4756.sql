set distributed_product_mode = 'local';

drop table if exists shard1;
drop table if exists shard2;
drop table if exists distr1;
drop table if exists distr2;

create table shard1 (id Int32) engine = MergeTree order by cityHash64(id);
create table shard2 (id Int32) engine = MergeTree order by cityHash64(id);

create table distr1 as shard1 engine Distributed (test_cluster_two_shards_localhost, currentDatabase(), shard1, cityHash64(id));
create table distr2 as shard2 engine Distributed (test_cluster_two_shards_localhost, currentDatabase(), shard2, cityHash64(id));

insert into shard1 (id) values (0), (1);
insert into shard2 (id) values (1), (2);

select distinct(distr1.id) from distr1
where distr1.id in
(
    select distr1.id
    from distr1
    join distr2 on distr1.id = distr2.id
    where distr1.id > 0
); -- { serverError 288 }

select distinct(d0.id) from distr1 d0
where d0.id in
(
    select d1.id
    from distr1 as d1
    join distr2 as d2 on d1.id = d2.id
    where d1.id > 0
);

-- TODO
--select distinct(distr1.id) from distr1
--where distr1.id in
--(
--    select distr1.id
--    from distr1 as d1
--    join distr2 as d2 on distr1.id = distr2.id
--    where distr1.id > 0
--);

drop table shard1;
drop table shard2;
drop table distr1;
drop table distr2;
