SELECT uniqExact(number::String) FROM numbers(10e6) GROUP BY (number%100)::String FORMAT Null SETTINGS max_bytes_ratio_before_external_group_by=-0.1; -- { serverError BAD_ARGUMENTS }
SELECT uniqExact(number::String) FROM numbers(10e6) GROUP BY (number%100)::String FORMAT Null SETTINGS max_bytes_ratio_before_external_group_by=1; -- { serverError BAD_ARGUMENTS }
