SET max_threads=1, max_bytes_before_external_group_by=0, max_memory_usage='300Mi';
-- Need non-UInt8 key for external aggregation, here String will be used
SELECT uniqExact(number::String) FROM numbers(10e6) GROUP BY (number%100)::String; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT uniqExact(number::String) FROM numbers(10e6) GROUP BY (number%100)::String SETTINGS max_bytes_ratio_before_external_group_by=0.5 FORMAT Null;
