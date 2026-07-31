-- A spilling join must use the temporary data scope of the running query,
-- so that `max_temporary_data_on_disk_size_for_query` is accounted for it.
-- Covers the new analyzer path, where the physical join is built from the logical join step.

SET enable_analyzer = 1;
SET join_algorithm = 'grace_hash';
SET max_bytes_in_join = 4000000;
SET max_threads = 1;

SET max_temporary_data_on_disk_size_for_query = 1024;
SELECT count() FROM numbers(1000000) AS t1 JOIN numbers(1000000) AS t2 ON t1.number = t2.number; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SET max_temporary_data_on_disk_size_for_query = 0;
SELECT count() FROM numbers(1000000) AS t1 JOIN numbers(1000000) AS t2 ON t1.number = t2.number;
