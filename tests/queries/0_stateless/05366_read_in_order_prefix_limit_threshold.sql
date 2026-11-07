-- Random settings limits: index_granularity=(None, 8192)
-- `ORDER BY` a prefix of the sorting key with a `LIMIT` that arms the threshold filter of `PartialSortingTransform`
-- (1500 <= limit <= block rows) must still stop reading early. https://github.com/ClickHouse/ClickHouse/issues/125296

DROP TABLE IF EXISTS t_small_groups;
DROP TABLE IF EXISTS t_large_groups;

CREATE TABLE t_small_groups (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_small_groups SELECT intDiv(number, 4), cityHash64(number) % 1000000 FROM numbers(100000);
OPTIMIZE TABLE t_small_groups FINAL;

CREATE TABLE t_large_groups (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_large_groups SELECT intDiv(number, 10000), cityHash64(number) % 1000000 FROM numbers(100000);
OPTIMIZE TABLE t_large_groups FINAL;

SELECT 'asc';
SELECT a, b FROM t_small_groups ORDER BY a, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0, log_comment = '05366_asc';
SELECT a, b FROM t_small_groups ORDER BY a, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 0, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0;

SELECT 'desc';
SELECT a, b FROM t_small_groups ORDER BY a DESC, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0, log_comment = '05366_desc';
SELECT a, b FROM t_small_groups ORDER BY a DESC, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 0, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0;

SELECT 'large_groups';
SELECT a, b FROM t_large_groups ORDER BY a, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0, log_comment = '05366_large_groups';
SELECT a, b FROM t_large_groups ORDER BY a, b LIMIT 2 OFFSET 1998
SETTINGS optimize_read_in_order = 0, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0;

SELECT 'exact_rows_before_limit';
SELECT a, b FROM t_small_groups ORDER BY a, b LIMIT 2 OFFSET 1998 FORMAT JSONCompact
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_block_size = 8192, enable_parallel_replicas = 0, ast_fuzzer_runs = 0,
    exact_rows_before_limit = 1, output_format_write_statistics = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'asc', read_rows < 50000 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05366_asc';
SELECT 'desc', read_rows < 50000 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05366_desc';
SELECT 'large_groups', read_rows < 50000 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05366_large_groups';

DROP TABLE t_small_groups;
DROP TABLE t_large_groups;
