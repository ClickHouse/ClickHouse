-- Tags: no-replicated-database, no-fasttest, no-shared-merge-tree
-- Tag no-replicated-database: different number of replicas

create table tableIn (n int)
    engine=ReplicatedMergeTree('/test/02916/{database}/table', '1')
    order by tuple()
    settings
        storage_policy='s3_cache',
        sleep_before_commit_local_part_in_replicated_table_ms=5000;
create table tableOut (n int)
    engine=ReplicatedMergeTree('/test/02916/{database}/table', '2')
    order by tuple()
    settings
        storage_policy='s3_cache';

SET send_logs_level='error';

insert into tableIn values(1);
insert into tableIn values(2);
system sync replica tableOut;
select count() from tableOut;

drop table tableIn;
drop table tableOut;
