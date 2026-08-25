-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/116333
-- A window function forces a Distributed read to stop at WithMergeableState, and such a boundary
-- carries no projection step, so its columns are ordered by first mention in the shard's
-- ALIAS-inlined query tree. That order differs from the order the initiator expects, and an ALIAS
-- column whose declared type differs from its body's type is not present on the shard at all (it
-- inlines to a _CAST over the raw column the shard does send). Reconciling those two headers
-- positionally silently returned values in the wrong columns, or raised
-- NUMBER_OF_COLUMNS_DOESNT_MATCH / CANNOT_PARSE_DATETIME.
--
-- Every distributed query below is paired with the equivalent local query: the local result is the
-- oracle, so a wrong-column result fails even though it raises no error.

DROP TABLE IF EXISTS loc_win;
DROP TABLE IF EXISTS dist_win;

CREATE TABLE loc_win
(
    a UInt64,
    cat LowCardinality(String),
    cur LowCardinality(String),
    dt DateTime,
    -- Declared type differs from the body's type, so this inlines to a _CAST the shard never emits.
    al String ALIAS cat,
    -- Declared type equals the body's type, so this inlines to the plain shard column `cat`.
    same LowCardinality(String) ALIAS cat,
    -- Body is an expression rather than a bare column.
    upper_al String ALIAS upper(cat),
    -- ALIAS over an ALIAS: inlining unwraps transitively.
    al_of_al String ALIAS al,
    nul Nullable(String) ALIAS cat
)
ENGINE = MergeTree ORDER BY a;

CREATE TABLE dist_win AS loc_win
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), loc_win, rand());

INSERT INTO loc_win (a, cat, cur, dt) VALUES (1, 'Empty', 'USD', '2024-01-01 00:00:00');

SET enable_analyzer = 1;

SELECT 'case 1 wrong results';
-- Reported case 1: the two projected columns came back swapped, with no error at all.
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM loc_win ORDER BY rn;

SELECT 'case 2 column count';
-- Reported case 2: NUMBER_OF_COLUMNS_DOESNT_MATCH (source: 3 and result: 4).
SELECT al, cat, dt, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al, cat, dt, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;

SELECT 'case 3 type mismatch';
-- Reported case 3: the positional pairing cast a String onto a DateTime (CANNOT_PARSE_DATETIME).
SELECT al, dt, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al, dt, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;

SELECT 'positional variants';
SELECT al, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
SELECT cur, al, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT cur, al, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
SELECT a, al, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT a, al, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
-- A lone ALIAS column was already correct; it must stay correct.
SELECT al, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;

SELECT 'alias body shapes';
-- Same-type ALIAS: resolved to the shard's own column by name.
SELECT same AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT same AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
-- Expression-bodied ALIAS: computed on the initiator from the raw column the shard sent.
SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
SELECT al_of_al AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al_of_al AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
SELECT nul AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT nul AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
-- Several ALIAS columns of different shapes at once.
SELECT al, same, upper_al, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT al, same, upper_al, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;

SELECT 'distributed engine table';
-- A real Distributed engine table takes the same path as remote().
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM dist_win ORDER BY rn;
SELECT al, cat, dt, row_number() OVER (ORDER BY a) AS rn FROM dist_win ORDER BY rn;
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM dist_win ORDER BY rn SETTINGS prefer_localhost_replica = 0;

SELECT 'window clauses';
SELECT al AS category, cur, sum(a) OVER (PARTITION BY cur ORDER BY dt) AS s
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY s;
SELECT al AS category, cur, sum(a) OVER (PARTITION BY cur ORDER BY dt) AS s
FROM loc_win ORDER BY s;
-- GROUP BY and a window function together take a different planner path; it must stay correct.
SELECT al AS category, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
GROUP BY al ORDER BY category;
SELECT al AS category, count() * 2 AS c FROM loc_win GROUP BY al ORDER BY category;

SELECT 'negative controls';
-- No ALIAS column: the two headers already agree, so nothing is reconstructed.
SELECT cat AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
SELECT cat AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
-- Declared but unreferenced ALIAS columns must not perturb the read.
SELECT cat, cur, dt, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY rn;
-- A single shard needs no reconciliation at all.
SELECT al AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.1', currentDatabase(), loc_win) ORDER BY rn;
-- An ALIAS column without a window function stops at FetchColumns, which reconciles by name.
SELECT al AS category, cur FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) ORDER BY category;
-- MATERIALIZED is a real stored column, so it is never inlined.
DROP TABLE IF EXISTS loc_mat;
CREATE TABLE loc_mat (a UInt64, cat LowCardinality(String), cur LowCardinality(String), dt DateTime,
                      mt String MATERIALIZED cat)
ENGINE = MergeTree ORDER BY a;
INSERT INTO loc_mat (a, cat, cur, dt) VALUES (1, 'Empty', 'USD', '2024-01-01 00:00:00');
SELECT mt AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_mat) ORDER BY rn;
SELECT mt AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_mat ORDER BY rn;
DROP TABLE loc_mat;

DROP TABLE dist_win;
DROP TABLE loc_win;
