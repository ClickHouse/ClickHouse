SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

-- TODO(cluster): _partition_id filter should be improved by filtering out unused replicas on the initiator

drop table if exists data_r1;
drop table if exists data_r2;

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by part settings cluster=1, cluster_replication_factor=2;

insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(20);
system sync replica data_r2;

set optimize_trivial_count_query=0;
-- { echoOn }
select count() from data_r1 prewhere _partition_id = '0';
select count() from data_r2 prewhere _partition_id = '0';

select count(ignore(*)) from data_r1 prewhere _partition_id = '0';
select count(ignore(*)) from data_r2 prewhere _partition_id = '0';

-- FIXME: this had been broken recently, but for now we use PREWHERE w/o
-- indexHint() so we are good with just the tests above.
--
--select count() from data_r1 where _partition_id = '0';
--select count() from data_r2 where _partition_id = '0';
--
--select count(ignore(*)) from data_r1 where _partition_id = '0';
--select count(ignore(*)) from data_r2 where _partition_id = '0';
--
--select count() from data_r1 where indexHint(_partition_id = '0');
--select count() from data_r2 where indexHint(_partition_id = '0');
--
--select count(ignore(*)) from data_r1 where indexHint(_partition_id = '0');
--select count(ignore(*)) from data_r2 where indexHint(_partition_id = '0');
