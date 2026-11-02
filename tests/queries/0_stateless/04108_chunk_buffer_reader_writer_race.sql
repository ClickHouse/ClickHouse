-- Regression test for a wrong-result / logical-error bug in correlated-subquery decorrelation.
-- The decorrelation builds an internal JOIN. When the user's join size limits are combined
-- with join_overflow_mode = 'break' (here max_bytes_in_join = 1), that internal join would
-- stop its build side early and drop rows. Besides giving a wrong subquery result, the early
-- finish lets the probe side start before the build side has consumed its input, which the
-- common-subplan in-memory buffer observes as
--   Logical error: Trying to extract chunk from ChunkBuffer before all inputs are finished
-- (and, depending on the consumer of the half-built right side, the sibling
--   Logical error: Trying to lookup values in runtime filter before building it was finished).
-- The fix makes the internal decorrelation join ignore the size limits and break mode.
--
-- Bug: https://github.com/ClickHouse/ClickHouse/issues/96445 (STID 2651-2cfd)

-- Correlated scalar subqueries are only supported by the analyzer, so force it on.
-- The old-analyzer CI profile sets allow_experimental_analyzer = 0 by default, which makes
-- the (SELECT arr WHERE 7) scope-capture of the outer-table column fail with
-- UNKNOWN_IDENTIFIER: Missing columns: 'arr' before the bug can even be exercised.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS chunk_buffer_race_input;

CREATE TABLE chunk_buffer_race_input
(
    key UInt64,
    arr Array(String)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 8192;

-- Use several separate INSERTs so the table has multiple parts. That makes ReadFromMergeTree
-- use its parallel thread pool and produce several parallel SaveSubqueryResultToBufferTransform
-- streams, the configuration that exposed the early-finish bug.
INSERT INTO chunk_buffer_race_input SELECT number,           ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   10000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   20000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   30000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   40000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   50000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   60000, ['a','b','c'] FROM numbers(2000);
INSERT INTO chunk_buffer_race_input SELECT number +   70000, ['a','b','c'] FROM numbers(2000);

-- Scalar correlated subquery over a scanned table, with the user join size limit set to 1 row
-- and join_overflow_mode = 'break'. Before the fix the internal decorrelation join inherited
-- these limits and short-circuited, aborting with the logical error above. We only care that
-- the server does not throw, so send the output to Null.
--
-- Repeat the query several times because the bug was timing-dependent: a single attempt often
-- won the scheduling lottery and missed it, but a handful of attempts reliably lost it.
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;
SELECT DISTINCT (SELECT arr WHERE 7) FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
    allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1
FORMAT `Null`;

-- A successful completion here is the assertion: the server must not throw an exception.
SELECT 1 AS ok;

-- Distinct-correlated-values variant. Decorrelation builds two internal joins: the final
-- "JOIN to generate result stream" and an earlier CROSS "JOIN to evaluate correlated
-- expression" that consumes the common-subplan reference carrying the correlated columns.
-- With max_rows_in_join = 1 and join_overflow_mode = 'break', a leaked size limit on either
-- internal join could drop correlated-key rows. Above the subquery returns a constant array, so
-- a drop would be invisible; here the subquery returns the key itself, so a dropped row changes
-- the result. The correlated result must equal the plain scan, so any silent drop on either
-- internal join is caught (1 = correct).
SELECT
(
    SELECT count(*) || '_' || sum(sub)
    FROM (SELECT key, (SELECT key WHERE 7) AS sub FROM chunk_buffer_race_input PREWHERE 38 QUALIFY -1)
    SETTINGS max_rows_in_join = 1, join_overflow_mode = 'break', join_algorithm = 'hash',
        allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, max_threads = 8
) = (SELECT count(*) || '_' || sum(key) FROM chunk_buffer_race_input) AS correlated_matches_plain_scan;

DROP TABLE chunk_buffer_race_input;
