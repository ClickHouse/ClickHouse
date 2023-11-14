
create table rmt (n int) engine=ReplicatedMergeTree('/test/02468/{database}', '1') order by tuple() partition by n % 2 settings replicated_max_ratio_of_wrong_parts=0, max_suspicious_broken_parts=0, max_suspicious_broken_parts_bytes=0;
create table rmt1 (n int) engine=ReplicatedMergeTree('/test/02468/{database}', '2') order by tuple() partition by n % 2 settings replicated_max_ratio_of_wrong_parts=0, max_suspicious_broken_parts=0, max_suspicious_broken_parts_bytes=0;

system stop cleanup rmt;
system stop merges rmt1;

insert into rmt select * from numbers(10) settings max_block_size=1;

alter table rmt drop partition id '0';
truncate table rmt1;

system sync replica rmt;
system sync replica rmt1;

detach table rmt sync;
detach table rmt1 sync;

attach table rmt;
attach table rmt1;

insert into rmt values (1);
insert into rmt1 values (2);
system sync replica rmt;
system sync replica rmt1;

select *, _table from merge(currentDatabase(), '') order by (*,), _table;
