
create table rmt (n int, ts DateTime64(8, 'UTC')) engine=ReplicatedMergeTree('/test/02487/{database}/rmt', '1') order by n;
alter table rmt add index idx1 date(ts) TYPE MinMax GRANULARITY 1;
create index idx2 on rmt date(ts) TYPE MinMax GRANULARITY 1;
system restart replica rmt;
create table rmt2 (n int, ts DateTime64(8, 'UTC'), index idx1 date(ts) TYPE MinMax GRANULARITY 1, index idx2 date(ts) TYPE MinMax GRANULARITY 1) engine=ReplicatedMergeTree('/test/02487/{database}/rmt', '2') order by n;
