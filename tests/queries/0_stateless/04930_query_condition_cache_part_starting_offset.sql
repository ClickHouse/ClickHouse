-- Tags: no-random-merge-tree-settings, no-random-settings

drop table if exists test_qcc_offset;
create table test_qcc_offset (key Int) engine=MergeTree() order by key;

system stop merges test_qcc_offset;
insert into test_qcc_offset select number from numbers(0, 100000);
insert into test_qcc_offset select number from numbers(100000, 100000);
insert into test_qcc_offset select number from numbers(200000, 100000);

set use_query_condition_cache=1;

-- { echo }
-- The starting offsets of the parts shift when a preceding part is dropped, so the granules
-- excluded for the previous part numbering must not be reused from the query condition cache
select count() from test_qcc_offset where _part_offset + _part_starting_offset < 50000;
alter table test_qcc_offset drop part 'all_1_1_0';
select count() from test_qcc_offset where _part_offset + _part_starting_offset < 50000;
-- The same for the filter that only reads the column (no index analysis is involved)
select count() from test_qcc_offset where _part_starting_offset < 100000;
alter table test_qcc_offset drop part 'all_2_2_0';
select count() from test_qcc_offset where _part_starting_offset < 100000;

-- { echoOff }

drop table test_qcc_offset;
