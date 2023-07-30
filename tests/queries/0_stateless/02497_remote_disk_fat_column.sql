-- Tags: no-random-settings, no-fasttest, no-tsan, no-asan, no-msan
set allow_suspicious_fixed_string_types=1;
create table fat_granularity (x UInt32, fat FixedString(160000)) engine = MergeTree order by x settings storage_policy = 's3_cache';

insert into fat_granularity select number, toString(number) || '_' from numbers(100000) settings max_block_size = 8192, max_insert_threads=8;

-- Too large sizes of FixedString to deserialize
select x from fat_granularity prewhere fat like '256\_%' settings max_threads=2;
