-- Single-thread coverage for the correlated-subquery decorrelation break-mode bug
-- (see 04108_chunk_buffer_reader_writer_race for the parallel case and full description).
-- With max_threads = 1 the executor schedules the build and probe sides of the internal
-- decorrelation join in a different order, so this is a distinct path worth exercising.
-- It also runs the original issue #96445 report shape (a scalar correlated subquery over a
-- GROUP BY ... WITH TOTALS source), whose pipeline contains the ReadFromCommonBufferSource /
-- SaveSubqueryResultToBufferTransform pair, so it genuinely drives the buffered reader rather
-- than only initializing the execution graph.
--
-- Bug: https://github.com/ClickHouse/ClickHouse/issues/96445 (STID 2651-2cfd)

-- Correlated scalar subqueries are only supported by the new analyzer, so force it on.
-- The old-analyzer CI profile sets allow_experimental_analyzer = 0 by default, which makes
-- the (SELECT arr WHERE 7) scope-capture of the outer-table column fail with
-- UNKNOWN_IDENTIFIER: Missing columns: 'arr' before the bug can even be exercised.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS chunk_buffer_max_threads_1_input;

CREATE TABLE chunk_buffer_max_threads_1_input
(
    key UInt64,
    arr Array(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 8192;

INSERT INTO chunk_buffer_max_threads_1_input SELECT number,         ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_max_threads_1_input SELECT number + 10000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_max_threads_1_input SELECT number + 20000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_max_threads_1_input SELECT number + 30000, ['a','b','c'] FROM numbers(2000);

-- The break-mode query from 04108, pinned to a single worker thread. Repeat a handful of
-- times because the original abort was timing-dependent.
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1
SETTINGS max_threads = 1, max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1
SETTINGS max_threads = 1, max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1
SETTINGS max_threads = 1, max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1
SETTINGS max_threads = 1, max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1
SETTINGS max_threads = 1, max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;

-- The original issue #96445 report shape: scalar correlated subquery over GROUP BY WITH TOTALS,
-- also at max_threads = 1. Its pipeline contains ReadFromCommonBufferSource /
-- SaveSubqueryResultToBufferTransform, so it genuinely drives the buffered reader. Sent to Null
-- because the result depends on the totals-row representation, which CI may format differently.
SELECT (SELECT count(*) FROM (SELECT t0.c0)) AS a0
FROM (SELECT 1 AS c0 GROUP BY 1 WITH TOTALS) AS t0
SETTINGS max_threads = 1, allow_experimental_correlated_subqueries = 1,
    correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;

-- A successful completion here is the assertion: the server must not throw or hang.
SELECT 1 AS ok;

-- Distinct-correlated-values variant at max_threads = 1 (see 04108 for the full description).
-- The subquery returns the key itself, so a dropped correlated-key row on either internal
-- decorrelation join changes the result. The correlated result must equal the plain scan
-- (1 = correct), catching a silent drop rather than only a crash.
SELECT
(
    SELECT count(*) || '_' || sum(sub)
    FROM (SELECT key, (SELECT key WHERE 7) AS sub FROM chunk_buffer_max_threads_1_input PREWHERE 38 QUALIFY -1)
    SETTINGS max_threads = 1, max_rows_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
        allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
) = (SELECT count(*) || '_' || sum(key) FROM chunk_buffer_max_threads_1_input) AS correlated_matches_plain_scan;

DROP TABLE chunk_buffer_max_threads_1_input;
