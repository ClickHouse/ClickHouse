-- Tags: no-fasttest, no-parallel

set output_format_orc_string_as_string = 1;
set output_format_orc_row_index_stride = 100;
set input_format_orc_row_batch_size = 100;
set input_format_orc_filter_push_down = 1;
set input_format_null_as_default = 1;

set engine_file_truncate_on_insert = 1;
set optimize_or_like_chain = 0;
set max_block_size = 100000;
set max_insert_threads = 1;

SET session_timezone = 'UTC';


-- Try all the types.
insert into function file('02892.orc')
    with 5000 - number as n
select
    number,
    intDiv(n, 11)::Int8 as i8,
    n::Int16 i16,
    n::Int32 as i32,
    n::Int64 as i64,

    toDate32(n*500000) as date32,
    toDateTime64(n*1e6, 3) as dt64_ms,
    toDateTime64(n*1e6, 6) as dt64_us,
    toDateTime64(n*1e6, 9) as dt64_ns,
    toDateTime64(n*1e6, 0) as dt64_s,
    toDateTime64(n*1e6, 2) as dt64_cs,
    (n/1000)::Float32 as f32,
    (n/1000)::Float64 as f64,
    n::String as s,
    n::String::FixedString(9) as fs,
    n::Decimal32(3)/1234 as d32,
    n::Decimal64(10)/12345678 as d64,
    n::Decimal128(20)/123456789012345 as d128
    from numbers(10000);

desc file('02892.orc');


-- Go over all types individually
-- { echoOn }
select count(), sum(number) from file('02892.orc') where indexHint(i8 in (10, 15, -6));
select count(1), min(i8), max(i8) from file('02892.orc') where i8 in (10, 15, -6);

select count(), sum(number) from file('02892.orc') where indexHint(i8 between -3 and 2);
select count(1), min(i8), max(i8) from file('02892.orc') where i8 between -3 and 2;

select count(), sum(number) from file('02892.orc') where indexHint(i16 between 4000 and 61000 or i16 == 42);
select count(1), min(i16), max(i16) from file('02892.orc') where i16 between 4000 and 61000 or i16 == 42;

select count(), sum(number) from file('02892.orc') where indexHint(i16 between -150 and 250);
select count(1), min(i16), max(i16) from file('02892.orc') where i16 between -150 and 250;

select count(), sum(number) from file('02892.orc') where indexHint(i32 in (42, -1000));
select count(1), min(i32), max(i32) from file('02892.orc') where i32 in (42, -1000);

select count(), sum(number) from file('02892.orc') where indexHint(i32 between -150 and 250);
select count(1), min(i32), max(i32) from file('02892.orc') where i32 between -150 and 250;

select count(), sum(number) from file('02892.orc') where indexHint(i64 in (42, -1000));
select count(1), min(i64), max(i64) from file('02892.orc') where i64 in (42, -1000);

select count(), sum(number) from file('02892.orc') where indexHint(i64 between -150 and 250);
select count(1), min(i64), max(i64) from file('02892.orc') where i64 between -150 and 250;

select count(), sum(number) from file('02892.orc') where indexHint(date32 between '1992-01-01' and '2023-08-02');
select count(1), min(date32), max(date32) from file('02892.orc') where date32 between '1992-01-01' and '2023-08-02';

select count(), sum(number) from file('02892.orc') where indexHint(dt64_ms between '2000-01-01' and '2005-01-01');
select count(1), min(dt64_ms), max(dt64_ms) from file('02892.orc') where dt64_ms between '2000-01-01' and '2005-01-01';

select count(), sum(number) from file('02892.orc') where indexHint(dt64_us between toDateTime64(900000000, 2) and '2005-01-01');
select count(1), min(dt64_us), max(dt64_us) from file('02892.orc') where (dt64_us between toDateTime64(900000000, 2) and '2005-01-01');

select count(), sum(number) from file('02892.orc') where indexHint(dt64_ns between '2000-01-01' and '2005-01-01');
select count(1), min(dt64_ns), max(dt64_ns) from file('02892.orc') where (dt64_ns between '2000-01-01' and '2005-01-01');

select count(), sum(number) from file('02892.orc') where indexHint(dt64_s between toDateTime64('-2.01e8'::Decimal64(0), 0) and toDateTime64(1.5e8::Decimal64(0), 0));
select count(1), min(dt64_s), max(dt64_s) from file('02892.orc') where (dt64_s between toDateTime64('-2.01e8'::Decimal64(0), 0) and toDateTime64(1.5e8::Decimal64(0), 0));

select count(), sum(number) from file('02892.orc') where indexHint(dt64_cs between toDateTime64('-2.01e8'::Decimal64(1), 1) and toDateTime64(1.5e8::Decimal64(2), 2));
select count(1), min(dt64_cs), max(dt64_cs) from file('02892.orc') where (dt64_cs between toDateTime64('-2.01e8'::Decimal64(1), 1) and toDateTime64(1.5e8::Decimal64(2), 2));

select count(), sum(number) from file('02892.orc') where indexHint(f32 between -0.11::Float32 and 0.06::Float32);
select count(1), min(f32), max(f32) from file('02892.orc') where (f32 between -0.11::Float32 and 0.06::Float32);

select count(), sum(number) from file('02892.orc') where indexHint(f64 between -0.11 and 0.06);
select count(1), min(f64), max(f64) from file('02892.orc') where (f64 between -0.11 and 0.06);

select count(), sum(number) from file('02892.orc') where indexHint(s between '-9' and '1!!!');
select count(1), min(s), max(s) from file('02892.orc') where (s between '-9' and '1!!!');

select count(), sum(number) from file('02892.orc') where indexHint(fs between '-9' and '1!!!');
select count(1), min(fs), max(fs) from file('02892.orc') where (fs between '-9' and '1!!!');

select count(), sum(number) from file('02892.orc') where indexHint(d32 between '-0.011'::Decimal32(3) and 0.006::Decimal32(3));
select count(1), min(d32), max(d32) from file('02892.orc') where (d32 between '-0.011'::Decimal32(3) and 0.006::Decimal32(3));

select count(), sum(number) from file('02892.orc') where indexHint(d64 between '-0.0000011'::Decimal64(7) and 0.0000006::Decimal64(9));
select count(1), min(d64), max(d64) from file('02892.orc') where (d64 between '-0.0000011'::Decimal64(7) and 0.0000006::Decimal64(9));

select count(), sum(number) from file('02892.orc') where indexHint(d128 between '-0.00000000000011'::Decimal128(20) and 0.00000000000006::Decimal128(20));
select count(1), min(d128), max(128) from file('02892.orc') where (d128 between '-0.00000000000011'::Decimal128(20) and 0.00000000000006::Decimal128(20));

-- Some random other cases.
select count(), sum(number) from file('02892.orc') where indexHint(0);
select count(), min(number), max(number) from file('02892.orc') where indexHint(0);

select count(), sum(number) from file('02892.orc') where indexHint(s like '99%' or i64 == 2000);
select count(), min(s), max(s) from file('02892.orc') where (s like '99%' or i64 == 2000);

select count(), sum(number) from file('02892.orc') where indexHint(s like 'z%');
select count(), min(s), max(s) from file('02892.orc') where (s like 'z%');

select count(), sum(number) from file('02892.orc') where indexHint(i8 == 10 or 1 == 1);
select count(), min(i8), max(i8) from file('02892.orc') where (i8 == 10 or 1 == 1);

select count(), sum(number) from file('02892.orc') where indexHint(i8 < 0);
select count(), min(i8), max(i8) from file('02892.orc') where (i8 < 0);
-- { echoOff }

-- Nullable and LowCardinality.
insert into function file('02892.orc') select
    number,
    if(number%234 == 0, NULL, number) as sometimes_null,
    toNullable(number) as never_null,
    if(number%345 == 0, number::String, NULL) as mostly_null,
    toLowCardinality(if(number%234 == 0, NULL, number)) as sometimes_null_lc,
    toLowCardinality(toNullable(number)) as never_null_lc,
    toLowCardinality(if(number%345 == 0, number::String, NULL)) as mostly_null_lc
    from numbers(1000);

-- { echoOn }
select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null is NULL);
select count(), min(sometimes_null), max(sometimes_null) from file('02892.orc') where (sometimes_null is NULL);

select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null_lc is NULL);
select count(), min(sometimes_null_lc), max(sometimes_null_lc) from file('02892.orc') where (sometimes_null_lc is NULL);

select count(), sum(number) from file('02892.orc') where indexHint(mostly_null is not NULL);
select count(), min(mostly_null), max(mostly_null) from file('02892.orc') where (mostly_null is not NULL);

select count(), sum(number) from file('02892.orc') where indexHint(mostly_null_lc is not NULL);
select count(), min(mostly_null_lc), max(mostly_null_lc) from file('02892.orc') where (mostly_null_lc is not NULL);

select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null > 850);
select count(), min(sometimes_null), max(sometimes_null) from file('02892.orc') where (sometimes_null > 850);

select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null_lc > 850);
select count(), min(sometimes_null_lc), max(sometimes_null_lc) from file('02892.orc') where (sometimes_null_lc > 850);

select count(), sum(number) from file('02892.orc') where indexHint(never_null > 850);
select count(), min(never_null), max(never_null) from file('02892.orc') where (never_null > 850);

select count(), sum(number) from file('02892.orc') where indexHint(never_null_lc > 850);
select count(), min(never_null_lc), max(never_null_lc) from file('02892.orc') where (never_null_lc > 850);

select count(), sum(number) from file('02892.orc') where indexHint(never_null < 150);
select count(), min(never_null), max(never_null) from file('02892.orc') where (never_null < 150);

select count(), sum(number) from file('02892.orc') where indexHint(never_null_lc < 150);
select count(), min(never_null_lc), max(never_null_lc) from file('02892.orc') where (never_null_lc < 150);

select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null < 150);
select count(), min(sometimes_null), max(sometimes_null) from file('02892.orc') where (sometimes_null < 150);

select count(), sum(number) from file('02892.orc') where indexHint(sometimes_null_lc < 150);
select count(), min(sometimes_null_lc), max(sometimes_null_lc) from file('02892.orc') where (sometimes_null_lc < 150);
-- { echoOff }

-- Settings that affect the table schema or contents.
insert into function file('02892.orc') select
    number,
    if(number%234 == 0, NULL, number + 100) as positive_or_null,
    if(number%234 == 0, NULL, -number - 100) as negative_or_null,
    if(number%234 == 0, NULL, 'I am a string') as string_or_null
    from numbers(1000);

-- { echoOn }
select count(), sum(number) from file('02892.orc') where indexHint(positive_or_null < 50); -- quirk with infinities
select count(), min(positive_or_null), max(positive_or_null) from file('02892.orc') where (positive_or_null < 50);

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, positive_or_null UInt64') where indexHint(positive_or_null < 50);
select count(), min(positive_or_null), max(positive_or_null) from file('02892.orc', ORC, 'number UInt64, positive_or_null UInt64') where (positive_or_null < 50);

select count(), sum(number) from file('02892.orc') where indexHint(negative_or_null > -50);
select count(), min(negative_or_null), max(negative_or_null) from file('02892.orc') where (negative_or_null > -50);

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where indexHint(negative_or_null > -50);
select count(), min(negative_or_null), max(negative_or_null) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where (negative_or_null > -50);

select count(), sum(number) from file('02892.orc') where indexHint(string_or_null == ''); -- quirk with infinities
select count(), min(string_or_null), max(string_or_null) from file('02892.orc') where (string_or_null == '');

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, string_or_null String') where indexHint(string_or_null == '');
select count(), min(string_or_null), max(string_or_null) from file('02892.orc', ORC, 'number UInt64, string_or_null String') where (string_or_null == '');

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, nEgAtIvE_oR_nUlL Int64') where indexHint(nEgAtIvE_oR_nUlL > -50) settings input_format_orc_case_insensitive_column_matching = 1;
select count(), min(nEgAtIvE_oR_nUlL), max(nEgAtIvE_oR_nUlL) from file('02892.orc', ORC, 'number UInt64, nEgAtIvE_oR_nUlL Int64') where (nEgAtIvE_oR_nUlL > -50) settings input_format_orc_case_insensitive_column_matching = 1;

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where indexHint(negative_or_null < -500);
select count(), min(negative_or_null), max(negative_or_null) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where (negative_or_null < -500);

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where indexHint(negative_or_null is null) settings allow_experimental_analyzer=1;
select count(), min(negative_or_null), max(negative_or_null) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where (negative_or_null is null);

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where indexHint(negative_or_null in (0, -1, -10, -100, -1000));
select count(), min(negative_or_null), max(negative_or_null) from file('02892.orc', ORC, 'number UInt64, negative_or_null Int64') where (negative_or_null in (0, -1, -10, -100, -1000));

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, string_or_null LowCardinality(String)') where indexHint(string_or_null like 'I am%');
select count(), min(string_or_null), max(string_or_null) from file('02892.orc', ORC, 'number UInt64, string_or_null LowCardinality(String)') where (string_or_null like 'I am%');

select count(), sum(number) from file('02892.orc', ORC, 'number UInt64, string_or_null LowCardinality(Nullable(String))') where indexHint(string_or_null like 'I am%');
select count(), min(string_or_null), max(string_or_null) from file('02892.orc', ORC, 'number UInt64, string_or_null LowCardinality(Nullable(String))') where (string_or_null like 'I am%');
-- { echoOff }
