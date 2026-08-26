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
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM loc_win ORDER BY rn;
SELECT al, cat, dt, row_number() OVER (ORDER BY a) AS rn FROM dist_win ORDER BY rn;
SELECT al, cat, dt, row_number() OVER (ORDER BY a) AS rn FROM loc_win ORDER BY rn;
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM dist_win ORDER BY rn SETTINGS prefer_localhost_replica = 0;
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM loc_win ORDER BY rn;

SELECT 'nested subquery';
-- A distributed read nested in a subquery is renumbered independently of the initiator's query tree
-- (`createUniqueAliasesIfNecessary` restarts the `__tableN` aliases at 1), so an expected column and
-- the shard column feeding it differ by that number as well as by the ALIAS inlining.
SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
) ORDER BY rn;
SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
    FROM loc_win
) ORDER BY rn;
-- An expression-bodied ALIAS is computed on the initiator, so its leaves have to resolve to the
-- shard's renumbered columns.
SELECT * FROM (
    SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
) ORDER BY rn;
SELECT * FROM (
    SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win
) ORDER BY rn;
-- The raw column the ALIAS body reads is selected alongside the ALIAS itself.
SELECT * FROM (
    SELECT al AS category, cat, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
) ORDER BY rn;
SELECT * FROM (
    SELECT al AS category, cat, row_number() OVER (ORDER BY a) AS rn FROM loc_win
) ORDER BY rn;
SELECT count() FROM (
    SELECT al, cat, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
);
SELECT count() * 2 FROM (
    SELECT al, cat, row_number() OVER (ORDER BY a) AS rn FROM loc_win
);
SELECT * FROM (SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
)) ORDER BY rn;
SELECT * FROM (SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win
)) ORDER BY rn;

SELECT 'deduplicated alias pair';
-- Two ALIAS columns with the same body collapse to one shard column, so the initiator fans that column
-- back out and reports the duplicate. Adding a third ALIAS column the shard does not send at all puts
-- the fan-out and the computed path in one header, which is where a duplicate reported by a mapping the
-- plan then declined would be applied to a plan that does not perform the collapse.
DROP TABLE IF EXISTS loc_dup;
CREATE TABLE loc_dup
(
    a UInt64,
    x UInt8,
    dt DateTime,
    s1 UInt8 ALIAS x,
    s2 UInt8 ALIAS x,
    cst String ALIAS x
)
ENGINE = MergeTree ORDER BY a;
INSERT INTO loc_dup (a, x, dt) VALUES (1, 7, '2024-01-01 00:00:00');
SELECT s1, s2, cst, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_dup) ORDER BY rn;
SELECT s1, s2, cst, row_number() OVER (ORDER BY a) AS rn FROM loc_dup ORDER BY rn;
SELECT s1, s2, row_number() OVER (ORDER BY dt) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_dup) ORDER BY rn;
SELECT s1, s2, row_number() OVER (ORDER BY dt) AS rn FROM loc_dup ORDER BY rn;
-- Nested, so the fan-out and the computed leaf both cross the qualifier renumbering.
SELECT * FROM (
    SELECT s1, s2, cst, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_dup)
) ORDER BY rn;
SELECT * FROM (
    SELECT s1, s2, cst, row_number() OVER (ORDER BY a) AS rn FROM loc_dup
) ORDER BY rn;
-- An aggregation over the collapsed pair consumes the reported duplicate mapping. Two-level aggregation
-- is pinned because the shard's bucket numbers depend on which key columns it deduplicated.
SELECT s1, s2, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), loc_dup)
GROUP BY s1, s2 ORDER BY s1
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
SELECT s1, s2, count() * 2 AS c FROM loc_dup GROUP BY s1, s2 ORDER BY s1;
SELECT s1, s2, cst, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), loc_dup)
GROUP BY s1, s2, cst ORDER BY s1
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
SELECT s1, s2, cst, count() * 2 AS c FROM loc_dup GROUP BY s1, s2, cst ORDER BY s1;
DROP TABLE loc_dup;

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
