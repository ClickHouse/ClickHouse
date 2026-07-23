-- Tags: no-parallel, no-fasttest

insert into function file('03630_parquet_bool_bug.parquet', Parquet, 'tags Array(Bool)') settings engine_file_truncate_on_insert=1 values ([false,false,false,false,false,false,false,false]), ([true,true,true,true,true,true,true,true]);
select sum(tags) from file('03630_parquet_bool_bug.parquet') array join tags settings input_format_parquet_use_native_reader_v3=1;

-- Try all 256 1-byte masks to verify the bit shifting nonsense in PlainBooleanDecoder.
insert into function file('03630_parquet_bool_bug.parquet') select number as n, arrayMap(i -> toBool(bitShiftRight(number, i) % 2 = 1), range(8)) as bits from numbers(256) settings engine_file_truncate_on_insert=1;
select sum(n = arraySum(arrayMap(i -> bitShiftLeft(bits[i+1], i), range(8)))) as ok from file('03630_parquet_bool_bug.parquet') settings input_format_parquet_use_native_reader_v3=1, schema_inference_make_columns_nullable=0;
