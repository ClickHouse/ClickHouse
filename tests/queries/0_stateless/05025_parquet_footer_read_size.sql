-- Tags: no-fasttest
-- Tests input_format_parquet_footer_read_size: the v3 reader sizes the initial footer read
-- adaptively when the setting is 0 (default), and uses the explicit value otherwise. Every
-- value - the adaptive default, a huge fixed read that covers the whole footer in one go, and a
-- tiny fixed read that forces the second (metadata_size + 8 > initial_read_size) read path - must
-- produce identical, correct results.

insert into function file(currentDatabase() || '_data_05025.parquet')
    select number as x, toString(number) as s from numbers(10000)
    settings engine_file_truncate_on_insert = 1;

-- Adaptive (default 0): 1% clamped to [128 KiB, 2 MiB].
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

-- Compatibility contract: the setting is new in 26.9 and its SettingsChangesHistory row records the
-- pre-26.9 behavior as the fixed 64 KiB footer read. SET compatibility to an older version must
-- restore that fixed 65536, not leak the new adaptive default (0) through.
set compatibility = '26.8';
select getSetting('input_format_parquet_footer_read_size');
