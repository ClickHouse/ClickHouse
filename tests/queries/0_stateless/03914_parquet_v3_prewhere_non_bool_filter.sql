-- Tags: no-fasttest

-- Regression test for an assertion failure in Parquet V3 reader when PREWHERE
-- produces a UInt8 filter with values > 1. The old assertion used std::accumulate
-- (which sums values) instead of countBytesInFilter (which counts non-zero values),
-- causing a mismatch when the filter column contained e.g. 5 instead of 1.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=2938a4bc37947f550260419a7f1aba60ea01ba82&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_debug%29

set input_format_parquet_use_native_reader_v3 = 1;
set engine_file_truncate_on_insert = 1;

-- Create a Parquet file with a UInt8 column containing values > 1 (and some zeros).
insert into function file(currentDatabase() || '_03914.parquet')
    select toUInt8(number % 7) as x, number as y
    from numbers(100);

-- PREWHERE x: the filter column is UInt8 with raw values like 0,1,2,3,4,5,6.
-- Before the fix, this triggered a LOGICAL_ERROR in debug builds because the
-- assertion summed filter values (0+1+2+3+4+5+6+...) instead of counting non-zero ones.
select sum(y) from file(currentDatabase() || '_03914.parquet') prewhere x;

-- Same but with optimize_move_to_prewhere disabled (the failing fuzzer query had this).
select sum(y) from file(currentDatabase() || '_03914.parquet') prewhere x settings optimize_move_to_prewhere = 0;

-- Also test with a WHERE clause present (as in the original fuzzer query).
select sum(y) from file(currentDatabase() || '_03914.parquet') prewhere x where y >= 50;
