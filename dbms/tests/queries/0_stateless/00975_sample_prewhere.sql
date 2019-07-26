drop table if exists test.sample_prewhere;
drop table if exists test.sample_prewhere_all;

create table if not exists test.sample_prewhere (date Date, id Int32, time Int64) engine = MergeTree partition by date order by (id, time, intHash64(time)) sample by intHash64(time);
insert into test.sample_prewhere values ('2019-01-01', 2, 1564028096);
insert into test.sample_prewhere values ('2019-01-01', 1, 1564028096);
insert into test.sample_prewhere values ('2019-01-02', 3, 1564028096);

create table if not exists test.sample_prewhere_all as test.sample_prewhere engine = Distributed('test_cluster_two_shards_localhost', 'test', 'sample_prewhere');

select id from test.sample_prewhere_all SAMPLE 1 where toDateTime(time) = '2019-07-20 00:00:00' limit 0, 1;
select id from test.sample_prewhere_all SAMPLE 1 where toDateTime(time) > '2019-07-20 00:00:00' limit 0, 1;
select id from test.sample_prewhere_all SAMPLE 1 where toDateTime(time) = '2019-07-20 00:00:00';
select count() from test.sample_prewhere_all SAMPLE 1 where toDateTime(time) > '2019-07-20 00:00:00' limit 0, 1;

drop table if exists test.sample_prewhere;
drop table if exists test.sample_prewhere_all;
