-- Tags: no-fasttest
-- ^ no-fasttest: the s3 table function is gated on build flags

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

CREATE TABLE t_file_engine (x UInt64) ENGINE = File(TSV);
INSERT INTO t_file_engine SELECT number FROM numbers(50000);

CREATE TABLE t_mergetree (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mergetree SELECT number FROM numbers(50000);

INSERT INTO FUNCTION s3('http://localhost:11111/test/05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
    SELECT number FROM numbers(50000) SETTINGS s3_truncate_on_insert = 1;

-- file() table function
SELECT * FROM file('05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_file_function', ast_fuzzer_runs = 0
FORMAT Null;

-- File engine
SELECT * FROM t_file_engine
SETTINGS log_queries = 1, log_comment = '05043_file_engine', ast_fuzzer_runs = 0
FORMAT Null;

-- url() table function
SELECT * FROM url('http://localhost:8123/?query=SELECT+number+FROM+numbers(50000)', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_url_function', ast_fuzzer_runs = 0
FORMAT Null;

-- s3() table function; the same source serves every other object storage, azureBlobStorage included
SELECT * FROM s3('http://localhost:11111/test/05043_' || currentDatabase() || '.tsv', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05043_s3_function', ast_fuzzer_runs = 0
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
SELECT
    cell,
    if(read_rows = 50000 AND selected_rows = read_rows AND selected_bytes = read_bytes,
        'ok',
        'fail: read_rows=' || toString(read_rows)
            || ' SelectedRows=' || toString(selected_rows)
            || ' read_bytes=' || toString(read_bytes)
            || ' SelectedBytes=' || toString(selected_bytes)) AS result
FROM
(
    SELECT log_comment AS cell,
        argMax(read_rows, event_time_microseconds) AS read_rows,
        argMax(read_bytes, event_time_microseconds) AS read_bytes,
        argMax(ProfileEvents['SelectedRows'], event_time_microseconds) AS selected_rows,
        argMax(ProfileEvents['SelectedBytes'], event_time_microseconds) AS selected_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND log_comment IN ('05043_file_function', '05043_file_engine', '05043_url_function',
            '05043_s3_function', '05043_mergetree', '05043_numbers')
    GROUP BY cell
)
ORDER BY cell;

DROP TABLE t_file_engine;
DROP TABLE t_mergetree;
