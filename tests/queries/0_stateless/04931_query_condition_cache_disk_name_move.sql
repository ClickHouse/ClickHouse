-- Tags: no-fasttest, no-random-merge-tree-settings, no-random-settings
-- no-fasttest: requires the s3_disk disk of the local_remote storage policy

set use_query_condition_cache=1;

drop table if exists test_qcc_move;

create table test_qcc_move (key Int) engine=MergeTree() order by key settings storage_policy='local_remote';
system stop merges test_qcc_move;
insert into test_qcc_move select number from numbers(100000);

-- { echo }
-- Moving a part to another disk keeps the part name, so the entries cached for a filter over the
-- future disk name (when it matched no rows) must not be reused after the move (when it matches
-- all rows). The condition on the key forces reading rows instead of selecting parts by disk name.
select count() from test_qcc_move where _disk_name = 's3_disk' or key < 0;
alter table test_qcc_move move part 'all_1_1_0' to disk 's3_disk';
select count() from test_qcc_move where _disk_name = 's3_disk' or key < 0;
-- { echoOff }

drop table test_qcc_move;
