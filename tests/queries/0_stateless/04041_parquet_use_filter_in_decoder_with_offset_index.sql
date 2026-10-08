-- Tags: no-fasttest
-- Regression test for nullptr dereference in Parquet reader when use_filter_in_decoder
-- path encounters pages with reset prefetch handles (filtered out by offset index).
-- https://github.com/ClickHouse/ClickHouse/issues/99676

set engine_file_truncate_on_insert = 1;

-- Create a parquet file with offset index and multiple small pages per row group.
-- Dictionary encoding is disabled so that use_filter_in_decoder can trigger
-- (it requires !column.page.is_dictionary_encoded).
-- The data has a filter column `key` and a value column `val`.
-- key = 1 for rows 0..49 and 150..199, key = 0 for rows 50..149.
-- This creates a pattern where:
--   Subgroup 1 (rows 0-99): some rows pass (0-49), some don't (50-99)
--   Subgroup 2 (rows 100-199): some don't (100-149), some pass (150-199)
-- In subgroup 2, pages covering rows 100-149 have no passing rows, so their
-- prefetch handles get reset. But use_filter_in_decoder (triggered because
-- column.page.initialized from subgroup 1) reads pages sequentially, hitting
-- the reset handles.
insert into function file(currentDatabase() || '04041.parquet')
    select
        (number < 50 or number >= 150)::UInt8 as key,
        number as val
    from numbers(200)
    settings output_format_parquet_data_page_size = 100,
             output_format_parquet_batch_size = 10,
             output_format_parquet_row_group_size = 200,
             output_format_parquet_write_page_index = 1,
             output_format_parquet_max_dictionary_size = 0;

-- Read with PREWHERE on key, small max_block_size to create multiple subgroups.
-- The PREWHERE creates a filter bitmap for the val column. In subgroup 2,
-- determinePagesToPrefetch resets prefetch handles for pages with no passing rows.
-- Then decodePrimitiveColumn for val enters the use_filter_in_decoder path
-- (page.initialized=true from subgroup 1) and crashes trying to access those pages.
select sum(val)
    from file(currentDatabase() || '04041.parquet')
    prewhere key
    settings input_format_parquet_max_block_size = 100,
             optimize_move_to_prewhere = 0;
