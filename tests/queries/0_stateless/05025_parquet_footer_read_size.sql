-- Tags: no-fasttest
-- Tests input_format_parquet_footer_read_size: when the setting is 0 (default) the v3 reader reads
-- a 64 KiB footer tail of a local file (and sizes it adaptively for other sources), and uses the
-- explicit value otherwise. Every value - the default, a huge fixed read that covers the whole
-- footer in one go, and a tiny fixed read that forces the second (metadata_size + 8 >
-- initial_read_size) read path - must produce identical, correct results.

insert into function file(currentDatabase() || '_data_05025.parquet')
    select number as x, toString(number) as s from numbers(10000)
    settings engine_file_truncate_on_insert = 1;

-- Default (0): a local file, so a 64 KiB tail.
select count(), sum(x), min(s), max(s) from file(currentDatabase() || '_data_05025.parquet', auto, 'x UInt64, s String')
    settings input_format_parquet_footer_read_size = 0;

-- Explicit large read: footer certainly fits in the initial read.
select count(), sum(x), min(s), max(s) from file(currentDatabase() || '_data_05025.parquet', auto, 'x UInt64, s String')
    settings input_format_parquet_footer_read_size = 8388608;

-- Explicit tiny read: forces the second read to fetch the rest of the footer.
select count(), sum(x), min(s), max(s) from file(currentDatabase() || '_data_05025.parquet', auto, 'x UInt64, s String')
    settings input_format_parquet_footer_read_size = 16;

-- Explicit value larger than the file: clamped to the file size, still correct.
select count(), sum(x), min(s), max(s) from file(currentDatabase() || '_data_05025.parquet', auto, 'x UInt64, s String')
    settings input_format_parquet_footer_read_size = 1073741824;

-- Explicit value below the 8-byte trailer: bumped up to 8, no out-of-bounds read.
select count(), sum(x), min(s), max(s) from file(currentDatabase() || '_data_05025.parquet', auto, 'x UInt64, s String')
    settings input_format_parquet_footer_read_size = 1;

-- A local file larger than the adaptive minimum (128 KiB) reads a 64 KiB tail by default, the same as an
-- explicit 65536.
insert into function file(currentDatabase() || '_tail_a_05025.parquet')
    select cityHash64(number) as x from numbers(100000) settings engine_file_truncate_on_insert = 1;
insert into function file(currentDatabase() || '_tail_b_05025.parquet')
    select cityHash64(number) as x from numbers(100000) settings engine_file_truncate_on_insert = 1;
select count() from file(currentDatabase() || '_tail_a_05025.parquet', Parquet, 'x UInt64')
    settings input_format_parquet_footer_read_size = 0, storage_file_read_method = 'pread',
             optimize_count_from_files = 1, optimize_trivial_count_query = 1, log_comment = '05025_tail_default';
select count() from file(currentDatabase() || '_tail_b_05025.parquet', Parquet, 'x UInt64')
    settings input_format_parquet_footer_read_size = 65536, storage_file_read_method = 'pread',
             optimize_count_from_files = 1, optimize_trivial_count_query = 1, log_comment = '05025_tail_explicit';
system flush logs query_log;
select log_comment, ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] from system.query_log
where event_date >= yesterday() and current_database = currentDatabase() and type = 'QueryFinish'
      and log_comment like '05025_tail_%'
order by log_comment;

-- Compatibility contract: the setting is new in 26.10 and its SettingsChangesHistory row records the
-- pre-26.9 behavior as the fixed 64 KiB footer read. SET compatibility to an older version must
-- restore that fixed 65536, not leak the new adaptive default (0) through.
set compatibility = '26.9';
select getSetting('input_format_parquet_footer_read_size');
