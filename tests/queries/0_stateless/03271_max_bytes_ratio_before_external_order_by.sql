select number from numbers(100e6) order by number format Null settings max_bytes_ratio_before_external_sort=-0.1; -- { serverError BAD_ARGUMENTS }
select number from numbers(100e6) order by number format Null settings max_bytes_ratio_before_external_sort=1; -- { serverError BAD_ARGUMENTS }
