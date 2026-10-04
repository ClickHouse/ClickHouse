-- Tags: no-random-merge-tree-settings, no-random-settings

set use_query_condition_cache=1;

drop table if exists test_qcc_offset;
drop table if exists test_qcc_index;
drop table if exists test_qcc_rename;
drop table if exists test_qcc_renamed;

create table test_qcc_offset (key Int) engine=MergeTree() order by key;
system stop merges test_qcc_offset;
insert into test_qcc_offset select number from numbers(0, 100000);
insert into test_qcc_offset select number from numbers(100000, 100000);
insert into test_qcc_offset select number from numbers(200000, 100000);

create table test_qcc_index (key Int) engine=MergeTree() order by key;
system stop merges test_qcc_index;
insert into test_qcc_index select number from numbers(0, 100000);
insert into test_qcc_index select number from numbers(100000, 100000);
insert into test_qcc_index select number from numbers(200000, 100000);

create table test_qcc_rename (key Int) engine=MergeTree() order by key;
insert into test_qcc_rename select number from numbers(100000);

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
-- The part indexes shift when a preceding part is dropped as well
select count() from test_qcc_index where _part_index < 1;
alter table test_qcc_index drop part 'all_1_1_0';
select count() from test_qcc_index where _part_index < 1;
-- Renaming the table keeps its parts, so the entries cached for a filter over the future name
-- (when it matched no rows) must not be reused after the rename (when it matches all rows)
select count() from test_qcc_rename where _table = 'test_qcc_renamed';
rename table test_qcc_rename to test_qcc_renamed;
select count() from test_qcc_renamed where _table = 'test_qcc_renamed';
-- { echoOff }

drop table test_qcc_offset;
drop table test_qcc_index;
drop table test_qcc_renamed;
