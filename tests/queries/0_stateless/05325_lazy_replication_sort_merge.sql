-- Sorting the result of a one-to-many JOIN merges the lazily replicated probe-side columns.
-- Each pair of queries must return the same rows in the same order with and without lazy replication.

DROP TABLE IF EXISTS lazy_sort_probe;
DROP TABLE IF EXISTS lazy_sort_build;
DROP TABLE IF EXISTS lazy_sort_dict;

CREATE TABLE lazy_sort_probe (id UInt64, s String, big String, ns Nullable(String), arr Array(String), m Map(String, String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lazy_sort_build (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lazy_sort_dict (id UInt64, k UInt64) ENGINE = Join(ALL, LEFT, id);

INSERT INTO lazy_sort_probe SELECT number, toString(number), concat(repeat('a', 1100), toString(number)), if(number % 7 = 0, NULL, concat('v', toString(number))), [toString(number), 'x'], map('k', toString(number)) FROM numbers(10000);
-- Every probe row matches 5 rows, and `k` is unique.
INSERT INTO lazy_sort_build SELECT number % 10000, number FROM numbers(50000);
INSERT INTO lazy_sort_dict SELECT number % 10000, number FROM numbers(50000);

-- Copies of a probe row are far apart in the sorted result.
SELECT 'scattered', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;
SELECT 'scattered', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;

SELECT 'scattered, one thread', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 1;
SELECT 'scattered, one thread', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 1;

SELECT 'scattered, external sort', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;
SELECT 'scattered, external sort', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;

SELECT 'scattered, limit', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k LIMIT 1000 OFFSET 500)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;
SELECT 'scattered, limit', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY b.k LIMIT 1000 OFFSET 500)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;

-- Copies of a probe row are adjacent in the sorted result, so the merges deduplicate them.
SELECT 'adjacent', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY p.id, b.k)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;
SELECT 'adjacent', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY p.id, b.k)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000;

SELECT 'adjacent, external sort', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY p.id, b.k)
SETTINGS enable_lazy_columns_replication = 0, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;
SELECT 'adjacent, external sort', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, b.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p INNER JOIN lazy_sort_build AS b ON b.id = p.id ORDER BY p.id, b.k)
SETTINGS enable_lazy_columns_replication = 1, query_plan_join_swap_table = false, max_threads = 4, max_block_size = 1000, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;

-- The probe table is read in key order and the join keeps it, so only the second sort key is sorted.
SELECT 'read in order', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, d.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p ALL LEFT JOIN lazy_sort_dict AS d USING (id) ORDER BY p.id, d.k)
SETTINGS enable_lazy_columns_replication = 0, optimize_read_in_order = 1, max_threads = 4, max_block_size = 1000;
SELECT 'read in order', count(), sum(cityHash64(rowNumberInAllBlocks(), id, k, s, big, ns, arr, m)) FROM (SELECT p.id AS id, d.k AS k, p.s AS s, p.big AS big, p.ns AS ns, p.arr AS arr, p.m AS m FROM lazy_sort_probe AS p ALL LEFT JOIN lazy_sort_dict AS d USING (id) ORDER BY p.id, d.k)
SETTINGS enable_lazy_columns_replication = 1, optimize_read_in_order = 1, max_threads = 4, max_block_size = 1000;

DROP TABLE lazy_sort_probe;
DROP TABLE lazy_sort_build;
DROP TABLE lazy_sort_dict;
