-- {echoOn}
select count(*) from file('simple-fd-bf.parquet', Parquet) SETTINGS use_cache_for_count_from_files=false;

-- bloom filter is off, all row groups should be read
select * from file('simple-fd-bf.parquet', Parquet)
    where f32=toFloat32(1.3040012)
        or f64=toFloat64(0.631960187601809)
        SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;

-- expect row count equals to select count()
select read_bytes, read_rows, result_rows, result_bytes from system.query_log where tables = ['_table_function.file'] and type = 2 order by event_time desc limit 1;

-- bloom filter is on, some row groups should be skipped
select * from file('simple-fd-bf.parquet', Parquet)
    where f32=toFloat32(1.3040012)
        or f64=toFloat64(0.631960187601809)
        SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;

-- expect row count much lower than select count()
select read_bytes, read_rows, result_rows, result_bytes from system.query_log where tables = ['_table_function.file'] and type = 2 order by event_time desc limit 1;

-- bloom filter is on and it is filtering by data in the other row group, which is bigger than the former. Expect most row groups to be read, but not all
select * from file('simple-fd-bf.parquet', Parquet) where f32 = toFloat32(1.7640524) and f64 = toFloat64(-0.48379749195754734) SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;

-- expect row count lower than select count(), but higher than above queries
select read_bytes, read_rows, result_rows, result_bytes from system.query_log where tables = ['_table_function.file'] and type = 2 order by event_time desc limit 1;


-- bloom filter is on, but includes data from both (all row groups), all row groups should be read
select * from file('simple-fd-bf.parquet', Parquet) where f32=toFloat32(1.3040012) or f64=toFloat64(0.631960187601809) or f32 = toFloat32(1.7640524) and f64 = toFloat64(-0.48379749195754734) SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;

-- expect row count equals to select count()
select read_bytes, read_rows, result_rows, result_bytes from system.query_log where tables = ['_table_function.file'] and type = 2 order by event_time desc limit 1;
