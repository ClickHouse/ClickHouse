-- Test for issue #105638: `max_temporary_data_on_disk_size_for_query` was not enforced
-- on external sort spill, because the spill scope was created from the server-global
-- temporary data scope instead of the scope of the query context that carries the
-- per-query and per-user limits.

select 'spill without per-query limit works';
select toString(cityHash64(number)) as s from numbers(2000000) order by s format Null
settings max_bytes_before_external_sort = 10000000, max_bytes_ratio_before_external_sort = 0, max_threads = 1;

select toString(cityHash64(number)) as s from numbers(2000000) order by s format Null
settings max_bytes_before_external_sort = 10000000, max_bytes_ratio_before_external_sort = 0, max_threads = 1,
         max_temporary_data_on_disk_size_for_query = 1000000; -- { serverError TOO_MANY_ROWS_OR_BYTES }
