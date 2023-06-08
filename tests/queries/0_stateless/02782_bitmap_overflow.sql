-- Tags: no-msan, no-asan

select unhex('0181808080908380808000')::AggregateFunction(groupBitmap, UInt64); -- {serverError TOO_LARGE_ARRAY_SIZE}

