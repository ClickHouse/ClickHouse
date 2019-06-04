set distributed_product_mode = 'local';

use test;
drop table if exists shard1;
drop table if exists shard2;
drop table if exists distr1;
drop table if exists distr2;

create table test.shard1 (id Int32) engine = MergeTree order by cityHash64(id);
create table test.shard2 (id Int32) engine = MergeTree order by cityHash64(id);

create table test.distr1 as shard1 engine Distributed (test_cluster_two_shards_localhost, test, shard1, cityHash64(id));
create table test.distr2 as shard2 engine Distributed (test_cluster_two_shards_localhost, test, shard2, cityHash64(id));

insert into shard1 (id) values (0), (1);
insert into shard2 (id) values (1), (2);

select distinct(test.distr1.id) from test.distr1
where test.distr1.id in
(
    select test.distr1.id
    from test.distr1
    join test.distr2 on test.distr1.id = test.distr2.id
    where test.distr1.id > 0
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
--select distinct(test.distr1.id) from test.distr1
--where test.distr1.id in
--(
--    select test.distr1.id
--    from test.distr1 as d1
--    join test.distr2 as d2 on test.distr1.id = test.distr2.id
--    where test.distr1.id > 0
--);

drop table shard1;
drop table shard2;
drop table distr1;
drop table distr2;
