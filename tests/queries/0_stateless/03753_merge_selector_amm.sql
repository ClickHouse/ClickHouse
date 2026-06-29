-- Tags: long

drop table if exists mt sync;

create table mt (a UInt64, b UInt64) engine=MergeTree order by a
settings
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once=1,
    max_parts_to_merge_at_once=10,
    parts_to_throw_insert=50;

insert into mt select number, number from numbers(100) settings max_block_size=1, min_insert_block_size_bytes=1;

select count() from mt;

drop table if exists mt sync;
