-- Tags: no-fasttest, no-msan
-- Tag no-fasttest, no-msan: the Vortex format is not included in the fast test and MSan builds

-- `input_format_vortex_preserve_order` in the `SETTINGS` of a table keeps the streams of its reads, as in 05361_storage_read_affinity_resize.

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1, enable_parallel_replicas = 0;
SET input_format_vortex_preserve_order = 0, parallelize_output_from_storages = 1;
SET engine_file_truncate_on_insert = 0, engine_file_allow_create_multiple_files = 1;

CREATE TABLE t_vortex (k UInt64) ENGINE = File(Vortex);
CREATE TABLE t_vortex_preserve_order (k UInt64) ENGINE = File(Vortex) SETTINGS input_format_vortex_preserve_order = 1;
INSERT INTO t_vortex SELECT number FROM numbers(1000);
INSERT INTO t_vortex SELECT number FROM numbers(10);
INSERT INTO t_vortex SELECT number FROM numbers(10);
INSERT INTO t_vortex_preserve_order SELECT number FROM numbers(1000);
INSERT INTO t_vortex_preserve_order SELECT number FROM numbers(10);
INSERT INTO t_vortex_preserve_order SELECT number FROM numbers(10);

SELECT countIf(trimLeft(explain) = 'Overflow × 2 1 → 2') FROM (EXPLAIN PIPELINE SELECT k FROM t_vortex);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_vortex_preserve_order);
SELECT countIf(trimLeft(explain) LIKE '%Resize%') FROM (EXPLAIN PIPELINE SELECT k FROM t_vortex_preserve_order) SETTINGS max_threads = 8;

DROP TABLE t_vortex;
DROP TABLE t_vortex_preserve_order;
