-- Tags: no-fasttest
-- no-fasttest: uses the S3 mock server on localhost:11111

-- The File, URL and object storage engines read their format through an inner pipeline and
-- report the rows again from the outer source, so `SelectedRows` and `SelectedBytes` must not be
-- accounted by the inner one. Every cell below reads a known number of rows, so the counters
-- have to equal `read_rows` and `read_bytes` of the same query. `currentDatabase` scopes the
-- server-global `query_log`, so concurrent copies of this test cannot read each other's rows.
--
-- `SelectedBytes` is asserted against `read_bytes` rather than against a computed constant: for a
-- file read it is the size consumed off the storage layer, not the materialised size of the rows
-- (a 50000-row UInt64 file is 288890 bytes on disk against 400000 in memory), and the two billing
-- layers reported those two different figures.
--
-- Both engine and table function forms are covered for File, and the MergeTree and `numbers`
-- cells are controls: a fix that deflated every read path would still have to leave them at 1x.
-- Each cell asserts its own row count too, so a cell cannot silently stop reading its source and
-- still report `ok`.
--
-- `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for
-- any query, and a fuzzed re-execution inherits `log_comment` and would win the `argMax` below.

DROP TABLE IF EXISTS t_file_engine;
DROP TABLE IF EXISTS t_mergetree;

INSERT INTO FUNCTION file('05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
    SELECT number FROM numbers(50000) SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('05043_' || currentDatabase() || '.parquet', Parquet, 'k UInt64, s String')
    SELECT number, concat('v', toString(number)) FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

CREATE TABLE t_file_engine (x UInt64) ENGINE = File(TSV);
INSERT INTO t_file_engine SELECT number FROM numbers(50000);

CREATE TABLE t_mergetree (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mergetree SELECT number FROM numbers(50000);

INSERT INTO FUNCTION s3('http://localhost:11111/test/05043_' || currentDatabase() || '.tsv', 'test', 'testtest', 'TSV', 'x UInt64')
    SELECT number FROM numbers(50000) SETTINGS s3_truncate_on_insert = 1;

-- A row-count cache entry is discarded again unless the file's mtime is strictly older than the
-- entry's registration time, and both have one-second granularity. The reads below therefore have
-- to start more than a second after the writes above, or the count cell would fall back to
-- parsing the file and would stop covering the cached-count pipeline.
SELECT sleep(2) FORMAT Null;

-- file() table function. This also populates the per-path row-count cache the count cell below
-- reads: `StorageFileSource::generate` stores `total_rows_in_file` once the file is exhausted.
SELECT * FROM file('05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_file_function', ast_fuzzer_runs = 0
FORMAT Null;

-- File engine
SELECT * FROM t_file_engine
SETTINGS log_queries = 1, log_comment = '05043_file_engine', ast_fuzzer_runs = 0
FORMAT Null;

-- url() and s3() are the two cells rewritten to their Cluster variants once parallel replicas
-- are enabled. The initiator then takes both counters from one remote progress packet, so those
-- two cells would report `ok` with the fix reverted; the rest of the test stays meaningful, so
-- the setting is pinned here instead of tagging the whole test.

-- url() table function
SELECT * FROM url('http://localhost:8123/?query=SELECT+number+FROM+numbers(50000)', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_url_function', ast_fuzzer_runs = 0,
    enable_parallel_replicas = 0
FORMAT Null;

-- s3() table function; the same source serves every other object storage, azureBlobStorage included
SELECT * FROM s3('http://localhost:11111/test/05043_' || currentDatabase() || '.tsv', 'test', 'testtest', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_s3_function', ast_fuzzer_runs = 0,
    enable_parallel_replicas = 0
FORMAT Null;

-- Row count served from the schema cache, which reads through a separate inner pipeline over
-- `ConstChunkGenerator` instead of parsing the file.
-- All three settings are pinned because the runner randomizes `optimize_count_from_files` and
-- `optimize_trivial_count_query` off with probability 0.05 each, and either one sends this query
-- back to parsing the file, which would silently stop covering the cached-count pipeline.
SELECT count() FROM file('05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_file_count_cache', ast_fuzzer_runs = 0,
    optimize_count_from_files = 1, use_cache_for_count_from_files = 1,
    optimize_trivial_count_query = 1
FORMAT Null;

-- Lazy materialization reads the deferred columns of the surviving rows in a second pass, through
-- an inner pipeline of its own. The two passes read 1000 + 3 rows, which is what arms this cell:
-- with the optimization off the same query reads 1000.
SELECT s FROM file('05043_' || currentDatabase() || '.parquet', Parquet, 'k UInt64, s String')
ORDER BY k LIMIT 3
SETTINGS log_queries = 1, log_comment = '05043_file_lazy_rows', ast_fuzzer_runs = 0,
    enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 0,
    query_plan_optimize_lazy_materialization_for_file = 1
FORMAT Null;

-- MergeTree control: has no inner pipeline on its read path and was never affected
SELECT * FROM t_mergetree
SETTINGS log_queries = 1, log_comment = '05043_mergetree', ast_fuzzer_runs = 0
FORMAT Null;

-- numbers() control
SELECT number FROM numbers(50000)
SETTINGS log_queries = 1, log_comment = '05043_numbers', ast_fuzzer_runs = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- `log_comment` is matched exactly rather than by prefix: the test runner injects a client-level
-- comment that also starts with 05043, and it would otherwise be measured too.
--
-- `expected_rows` pins each cell to the rows it must have read, so a cell cannot silently stop
-- reading its source and still report `ok`. The two cells that need more than a row count carry
-- an extra arming term: the count cell must have been served from the cache rather than by
-- parsing (a TSV `UInt64` cannot be parsed at less than two bytes per row, so `read_bytes` below
-- `read_rows` can only come from the fabricated chunks), and the lazy cell's 1003 rows are
-- themselves the proof that its second pass ran.
SELECT
    cell,
    if(read_rows = expected_rows AND armed AND selected_rows = read_rows AND selected_bytes = read_bytes,
        'ok',
        'fail: read_rows=' || toString(read_rows)
            || ' expected_rows=' || toString(expected_rows)
            || ' armed=' || toString(armed)
            || ' SelectedRows=' || toString(selected_rows)
            || ' read_bytes=' || toString(read_bytes)
            || ' SelectedBytes=' || toString(selected_bytes)) AS result
FROM
(
    SELECT log_comment AS cell,
        argMax(read_rows, event_time_microseconds) AS read_rows,
        argMax(read_bytes, event_time_microseconds) AS read_bytes,
        argMax(ProfileEvents['SelectedRows'], event_time_microseconds) AS selected_rows,
        argMax(ProfileEvents['SelectedBytes'], event_time_microseconds) AS selected_bytes,
        if(cell = '05043_file_lazy_rows', 1003, 50000) AS expected_rows,
        if(cell = '05043_file_count_cache', read_bytes < read_rows, 1) AS armed
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND log_comment IN ('05043_file_function', '05043_file_engine', '05043_url_function',
            '05043_s3_function', '05043_file_count_cache', '05043_file_lazy_rows',
            '05043_mergetree', '05043_numbers')
    GROUP BY cell
)
ORDER BY cell;

DROP TABLE t_file_engine;
DROP TABLE t_mergetree;
