DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    part Int32,
    key String
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY key
SETTINGS
    index_granularity = 1000000000,
    index_granularity_bytes = 100000,
    min_bytes_for_wide_part = 0,
    use_const_adaptive_granularity = 1,
    enable_index_granularity_compression = 1,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = '',
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    string_serialization_version = 'single_stream',
    merge_max_block_size = 65535;

-- { echo }
insert into tab select 1, if(number < 4096, 'foo', repeat('b', 1000)) from numbers(400e3) settings max_block_size=65535, max_insert_threads=1;
select name, rows, marks, index_granularity_bytes_in_memory_allocated from system.parts where database = currentDatabase() and table = 'tab' and active;
optimize table tab;
select name, rows, marks, index_granularity_bytes_in_memory_allocated from system.parts where database = currentDatabase() and table = 'tab' and active;
select * from tab format Null;
check table tab;
