-- External sort must spill into the temporary data scope of the running query,
-- so that `max_temporary_data_on_disk_size_for_query` is accounted for it.

SET max_threads = 1;
SET max_bytes_before_external_sort = 1;
SET max_bytes_ratio_before_external_sort = 0;

SET max_temporary_data_on_disk_size_for_query = 1024;
SELECT number FROM numbers(1000000) ORDER BY number DESC FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SET max_temporary_data_on_disk_size_for_query = 0;
SELECT count() FROM (SELECT number FROM numbers(1000000) ORDER BY number DESC);
