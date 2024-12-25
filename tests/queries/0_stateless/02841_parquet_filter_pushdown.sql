-- Tags: no-fasttest, no-parallel

set output_format_parquet_row_group_size = 100;

set input_format_null_as_default = 1;
set engine_file_truncate_on_insert = 1;
set optimize_or_like_chain = 0;
set max_block_size = 100000;
set max_insert_threads = 1;

-- Try all the types.
insert into function file('02841.parquet')
    -- Use negative numbers to test sign extension for signed types and lack of sign extension for
    -- unsigned types.
    with 5000 - number as n select

    number,

    intDiv(n, 11)::UInt8 as u8,
    n::UInt16 u16,
    n::UInt32 as u32,
    n::UInt64 as u64,
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
    n::Decimal128(20)/123456789012345 as d128,
    n::Decimal256(40)/123456789012345/678901234567890 as d256

    from numbers(10000);

desc file('02841.parquet');

-- To generate reference results, use a temporary table and GROUP BYs to simulate row group filtering:
--   create temporary table t as with [as above] select intDiv(number, 100) as group, [as above];
-- then e.g. for a query that filters by `x BETWEEN a AND b`:
--   select sum(c), sum(h) from (select count() as c, sum(number) as h, min(x) as mn, max(x) as mx from t group by group) where a <= mx and b >= mn;

-- Go over all types individually.
select count(), sum(number) from file('02841.parquet') where indexHint(u8 in (10, 15, 250));
select count(), sum(number) from file('02841.parquet') where indexHint(i8 between -3 and 2);
select count(), sum(number) from file('02841.parquet') where indexHint(u16 between 4000 and 61000 or u16 == 42);
select count(), sum(number) from file('02841.parquet') where indexHint(i16 between -150 and 250);
select count(), sum(number) from file('02841.parquet') where indexHint(u32 in (42, 4294966296));
select count(), sum(number) from file('02841.parquet') where indexHint(i32 between -150 and 250);
select count(), sum(number) from file('02841.parquet') where indexHint(u64 in (42, 18446744073709550616));
select count(), sum(number) from file('02841.parquet') where indexHint(i64 between -150 and 250);
select count(), sum(number) from file('02841.parquet') where indexHint(date32 between '1992-01-01' and '2023-08-02');
select count(), sum(number) from file('02841.parquet') where indexHint(dt64_ms between '2000-01-01' and '2005-01-01');
select count(), sum(number) from file('02841.parquet') where indexHint(dt64_us between toDateTime64(900000000, 2) and '2005-01-01');
select count(), sum(number) from file('02841.parquet') where indexHint(dt64_ns between '2000-01-01' and '2005-01-01');
select count(), sum(number) from file('02841.parquet') where indexHint(dt64_s between toDateTime64('-2.01e8'::Decimal64(0), 0) and toDateTime64(1.5e8::Decimal64(0), 0));
select count(), sum(number) from file('02841.parquet') where indexHint(dt64_cs between toDateTime64('-2.01e8'::Decimal64(1), 1) and toDateTime64(1.5e8::Decimal64(2), 2));
select count(), sum(number) from file('02841.parquet') where indexHint(f32 between -0.11::Float32 and 0.06::Float32);
select count(), sum(number) from file('02841.parquet') where indexHint(f64 between -0.11 and 0.06);
select count(), sum(number) from file('02841.parquet') where indexHint(s between '-9' and '1!!!');
select count(), sum(number) from file('02841.parquet') where indexHint(fs between '-9' and '1!!!');
select count(), sum(number) from file('02841.parquet') where indexHint(d32 between '-0.011'::Decimal32(3) and 0.006::Decimal32(3));
select count(), sum(number) from file('02841.parquet') where indexHint(d64 between '-0.0000011'::Decimal64(7) and 0.0000006::Decimal64(9));
select count(), sum(number) from file('02841.parquet') where indexHint(d128 between '-0.00000000000011'::Decimal128(20) and 0.00000000000006::Decimal128(20));
select count(), sum(number) from file('02841.parquet') where indexHint(d256 between '-0.00000000000000000000000000011'::Decimal256(40) and 0.00000000000000000000000000006::Decimal256(35));

-- Some random other cases.
select count(), sum(number) from file('02841.parquet') where indexHint(0);
select count(), sum(number) from file('02841.parquet') where indexHint(s like '99%' or u64 == 2000);
select count(), sum(number) from file('02841.parquet') where indexHint(s like 'z%');
select count(), sum(number) from file('02841.parquet') where indexHint(u8 == 10 or 1 == 1);
select count(), sum(number) from file('02841.parquet') where indexHint(u8 < 0);
select count(), sum(number) from file('02841.parquet') where indexHint(u64 + 1000000 == 1001000);
select count(), sum(number) from file('02841.parquet') where indexHint(u64 + 1000000 == 1001000) settings input_format_parquet_filter_push_down = 0;
select count(), sum(number) from file('02841.parquet') where indexHint(u32 + 1000000 == 999000);

-- Very long string, which makes the Parquet encoder omit the corresponding min/max stat.
insert into function file('02841.parquet')
    select arrayStringConcat(range(number*1000000)) as s from numbers(2);
select count() from file('02841.parquet') where indexHint(s > '');

-- Nullable and LowCardinality.
insert into function file('02841.parquet') select
    number,
    if(number%234 == 0, NULL, number) as sometimes_null,
    toNullable(number) as never_null,
    if(number%345 == 0, number::String, NULL) as mostly_null,
    toLowCardinality(if(number%234 == 0, NULL, number)) as sometimes_null_lc,
    toLowCardinality(toNullable(number)) as never_null_lc,
    toLowCardinality(if(number%345 == 0, number::String, NULL)) as mostly_null_lc
    from numbers(1000);

select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null is NULL);
select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null_lc is NULL);
select count(), sum(number) from file('02841.parquet') where indexHint(mostly_null is not NULL);
select count(), sum(number) from file('02841.parquet') where indexHint(mostly_null_lc is not NULL);
select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null > 850);
select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null_lc > 850);
select count(), sum(number) from file('02841.parquet') where indexHint(never_null > 850);
select count(), sum(number) from file('02841.parquet') where indexHint(never_null_lc > 850);
select count(), sum(number) from file('02841.parquet') where indexHint(never_null < 150);
select count(), sum(number) from file('02841.parquet') where indexHint(never_null_lc < 150);
-- Quirk with infinities: this reads too much because KeyCondition represents NULLs as infinities.
select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null < 150);
select count(), sum(number) from file('02841.parquet') where indexHint(sometimes_null_lc < 150);

-- Settings that affect the table schema or contents.
insert into function file('02841.parquet') select
    number,
    if(number%234 == 0, NULL, number + 100) as positive_or_null,
    if(number%234 == 0, NULL, -number - 100) as negative_or_null,
    if(number%234 == 0, NULL, 'I am a string') as string_or_null
    from numbers(1000);

select count(), sum(number) from file('02841.parquet') where indexHint(positive_or_null < 50); -- quirk with infinities
select count(), sum(number) from file('02841.parquet', Parquet, 'number UInt64, positive_or_null UInt64') where indexHint(positive_or_null < 50);
select count(), sum(number) from file('02841.parquet') where indexHint(negative_or_null > -50);
select count(), sum(number) from file('02841.parquet', Parquet, 'number UInt64, negative_or_null Int64') where indexHint(negative_or_null > -50);
select count(), sum(number) from file('02841.parquet') where indexHint(string_or_null == ''); -- quirk with infinities
select count(), sum(number) from file('02841.parquet', Parquet, 'number UInt64, string_or_null String') where indexHint(string_or_null == '');
select count(), sum(number) from file('02841.parquet', Parquet, 'number UInt64, nEgAtIvE_oR_nUlL Int64') where indexHint(nEgAtIvE_oR_nUlL > -50) settings input_format_parquet_case_insensitive_column_matching = 1;

-- Bad type conversions.
insert into function file('02841.parquet') select 42 as x;
select * from file('02841.parquet', Parquet, 'x Nullable(String)') where x not in (1);
insert into function file('t.parquet', Parquet, 'x String') values ('1'), ('100'), ('2');
select * from file('t.parquet', Parquet, 'x Int64') where x >= 3;
