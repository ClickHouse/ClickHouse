-- Tags: no-fasttest, no-parallel

set output_format_parquet_use_custom_encoder = 1;
set input_format_parquet_use_native_reader_v3 = 1;
set engine_file_truncate_on_insert = 1;

insert into function file('03596_parquet_prewhere_page_skip_bug.parquet') select number as n, number*10 as n10 from numbers(200) settings output_format_parquet_data_page_size=100, output_format_parquet_batch_size=10, output_format_parquet_row_group_size=100, output_format_parquet_write_page_index=0;

select n10 from file('03596_parquet_prewhere_page_skip_bug.parquet') prewhere n in (131, 174, 175, 176) order by all settings input_format_parquet_page_filter_push_down=0, input_format_parquet_filter_push_down=0, input_format_parquet_bloom_filter_push_down=0, input_format_parquet_max_block_size=10;
