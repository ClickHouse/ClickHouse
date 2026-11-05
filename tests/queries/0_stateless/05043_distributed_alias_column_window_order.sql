-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/116333
-- A window function forces a read to stop at WithMergeableState, reached both by a Distributed or
-- remote() read of two or more shards and by a parallel-replicas read of a plain table. Such a
-- boundary carries no projection step, so its columns are ordered by first mention in the shard's
-- ALIAS-inlined query tree. That order differs from the order the initiator expects, and an ALIAS
-- column whose declared type differs from its body's type is not present on the shard at all (it
-- inlines to a _CAST over the raw column the shard does send). Reconciling those two headers
-- positionally silently returned values in the wrong columns, or raised
-- NUMBER_OF_COLUMNS_DOESNT_MATCH / CANNOT_PARSE_DATETIME.
--
-- An arm with a comparable single-node equivalent is followed by it as the oracle, so a result in
-- the wrong columns fails even though it raises no error. Some arms deliberately have none, the
-- expected-error one necessarily so.

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

SELECT 'server dependent alias body';
-- An ALIAS body whose value depends on which server evaluates it is not a function of the shard's
-- columns, so it cannot be reconstructed from the shard header: the read is rejected instead of
-- answering with the initiator's value. Every shard here is the same server, so no value oracle can
-- tell the two answers apart, and each arm pins the rejection. `hostName` is constant-folded during
-- analysis while `rand64` stays a function call, which are the two node shapes carrying the property.
DROP TABLE IF EXISTS loc_srv;
CREATE TABLE loc_srv (a UInt64, x String,
                      srv String ALIAS concat(x, '_', hostName()),
                      nd String ALIAS concat(x, '_', toString(rand64())))
ENGINE = MergeTree ORDER BY a;
INSERT INTO loc_srv (a, x) VALUES (1, 'row');
SELECT srv, x, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_srv) ORDER BY rn; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT nd, x, row_number() OVER (ORDER BY a) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_srv) ORDER BY rn; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
-- Without a window function the whole query reaches the shard, which evaluates the body itself.
SELECT srv = concat(x, '_', hostName()) FROM remote('127.0.0.{1,2}', currentDatabase(), loc_srv) ORDER BY x;
DROP TABLE loc_srv;

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
SELECT * FROM (SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (ORDER BY a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
)) ORDER BY rn;
SELECT * FROM (SELECT * FROM (
    SELECT al AS category, cur, row_number() OVER (ORDER BY a) AS rn FROM loc_win
)) ORDER BY rn;
-- A CTE is a subquery scope as well, so it crosses the same renumbering.
WITH cte AS (SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
             FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win))
SELECT * FROM cte ORDER BY rn;
WITH cte AS (SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
             FROM loc_win)
SELECT * FROM cte ORDER BY rn;

SELECT 'deduplicated alias pair';
-- Two ALIAS columns with the same body collapse to one shard column, so the initiator fans that column
-- back out and reports the duplicate. Adding a third ALIAS column the shard does not send at all puts the
-- fan-out and the computed path in one header. Every mapping in this block is accepted; a mapping that
-- reports a duplicate and then declines is reached in the `joined sources` section.
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
-- GROUP BY with a window function: the shard aggregates, and the initiator adds the window on top of
-- the merged result. Both headers list the group key first here, so the mapping declines to the identity.
SELECT al AS category, count() AS c, row_number() OVER (ORDER BY al) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win) GROUP BY al ORDER BY category;
SELECT al AS category, count() * 2 AS c, row_number() OVER (ORDER BY al) AS rn
FROM loc_win GROUP BY al ORDER BY category;
-- GROUP BY alone also stops at a mergeable-state boundary, so it reaches the same reconciliation with
-- no window function anywhere in the query.
SELECT al AS category, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), loc_win)
GROUP BY al ORDER BY category;
SELECT al AS category, count() * 2 AS c FROM loc_win GROUP BY al ORDER BY category;

SELECT 'parallel replicas';
-- Reading a plain MergeTree table over parallel replicas stops at the same mergeable-state boundary, so
-- the same reconciliation runs with no Distributed table and no remote() in the query. Replicas split the
-- work instead of duplicating rows, so each arm returns the same rows as its single-replica oracle.
-- Every setting a task-based parallel read requires is pinned per statement, including the analyzer:
-- a server whose profile disables it refuses the parallel plan outright, and the rows alone do not
-- say which plan produced them, so each arm asserts below that replicas were really used. Only the
-- query-based path reads at that boundary, so the plan-based one is pinned off too: it splits the read
-- later in the plan, and the assertion below is positive either way.
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM loc_win ORDER BY rn
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
         automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0, enable_analyzer = 1,
         log_comment = '05043_pr_alias';
SELECT al AS category, cur, row_number() OVER (PARTITION BY a ORDER BY dt DESC) AS rn
FROM loc_win ORDER BY rn SETTINGS enable_parallel_replicas = 0;
-- The same over an expression-bodied ALIAS, whose body is computed on the initiator.
SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM loc_win ORDER BY rn
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
         automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0, enable_analyzer = 1,
         log_comment = '05043_pr_upper_alias';
SELECT upper_al AS category, cur, row_number() OVER (ORDER BY a) AS rn
FROM loc_win ORDER BY rn SETTINGS enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;
-- One aggregate row per arm however many rows carry the comment: a re-execution of the same query
-- inherits it.
SELECT max(ProfileEvents['ParallelReplicasUsedCount']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05043_pr_alias'
  AND type = 'QueryFinish' AND initial_query_id = query_id
SETTINGS enable_parallel_replicas = 0;
SELECT max(ProfileEvents['ParallelReplicasUsedCount']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05043_pr_upper_alias'
  AND type = 'QueryFinish' AND initial_query_id = query_id
SETTINGS enable_parallel_replicas = 0;

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

SELECT 'joined sources';
-- Both joined sources expose a column of the same name, so the shard header carries two columns whose
-- names differ only in the qualifier number (nesting is what makes them differ: the shard restarts its
-- `__tableN` aliases at 1). A leaf of an ALIAS body then cannot be attributed to one source, not even by
-- an exact name match, because the shard column of that exact name may belong to the other source. Such
-- a header is reconciled positionally instead, which passes each shard column through unchanged, so the
-- value-preserving ALIAS body keeps the result comparable to the single-node oracle. A body that
-- transforms its column returns it untransformed here, which is why these bodies preserve their value.
DROP TABLE IF EXISTS loc_jl;
DROP TABLE IF EXISTS loc_jr;
CREATE TABLE loc_jl (a UInt64, x String, al String ALIAS concat(x, '')) ENGINE = MergeTree ORDER BY a;
CREATE TABLE loc_jr (a UInt64, x String, al String ALIAS concat(x, '')) ENGINE = MergeTree ORDER BY a;
INSERT INTO loc_jl VALUES (1, 'left');
INSERT INTO loc_jr VALUES (1, 'right');
SELECT * FROM (
    SELECT l.al AS la, r.al AS ra, row_number() OVER (ORDER BY l.a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_jl) AS l
    GLOBAL INNER JOIN remote('127.0.0.{1,2}', currentDatabase(), loc_jr) AS r ON l.a = r.a
) ORDER BY rn;
SELECT * FROM (
    SELECT l.al AS la, r.al AS ra, row_number() OVER (ORDER BY l.a) AS rn
    FROM loc_jl AS l INNER JOIN loc_jr AS r ON l.a = r.a
) ORDER BY rn
SETTINGS enable_parallel_replicas = 0;
-- A plain JOIN of the two distributed sources reaches the same boundary.
SELECT * FROM (
    SELECT l.al AS la, r.al AS ra, row_number() OVER (ORDER BY l.a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_jl) AS l
    INNER JOIN remote('127.0.0.{1,2}', currentDatabase(), loc_jr) AS r ON l.a = r.a
) ORDER BY rn;
SELECT * FROM (
    SELECT l.al AS la, r.al AS ra, row_number() OVER (ORDER BY l.a) AS rn
    FROM loc_jl AS l INNER JOIN loc_jr AS r ON l.a = r.a
) ORDER BY rn
SETTINGS enable_parallel_replicas = 0;
DROP TABLE loc_jl;
DROP TABLE loc_jr;
-- A same-body ALIAS pair collapses to one shard column, so a duplicate is already recorded when a later
-- ALIAS body turns out to read a column both sources expose. Declining then leaves one expected column
-- with nothing to read, and reconciling positionally cannot invent it, so the query is rejected instead
-- of returning values from the wrong source. The arm pins that rejection, not the state of the reported
-- duplicates: this header carries one more expected column than the shard sends, so the read never
-- reaches an aggregation, which is the only thing that reads them.
DROP TABLE IF EXISTS loc_jdl;
DROP TABLE IF EXISTS loc_jdr;
CREATE TABLE loc_jdl (a UInt64, x String, y UInt8, s1 UInt8 ALIAS y, s2 UInt8 ALIAS y,
                      ex String ALIAS concat(x, ''))
ENGINE = MergeTree ORDER BY a;
CREATE TABLE loc_jdr (a UInt64, x String, ex2 String ALIAS concat(x, '')) ENGINE = MergeTree ORDER BY a;
INSERT INTO loc_jdl (a, x, y) VALUES (1, 'left', 7);
INSERT INTO loc_jdr (a, x) VALUES (1, 'right');
SELECT * FROM (
    SELECT l.s1, l.s2, l.ex, r.ex2, row_number() OVER (ORDER BY l.a) AS rn
    FROM remote('127.0.0.{1,2}', currentDatabase(), loc_jdl) AS l
    GLOBAL INNER JOIN remote('127.0.0.{1,2}', currentDatabase(), loc_jdr) AS r ON l.a = r.a
) ORDER BY rn; -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
-- Reading over parallel replicas puts even a single-node query on a mergeable-state boundary, so it stops
-- being a single-node oracle and reaches the same undecidable header; pinned off so this states the value.
SELECT * FROM (
    SELECT l.s1, l.s2, l.ex, r.ex2, row_number() OVER (ORDER BY l.a) AS rn
    FROM loc_jdl AS l INNER JOIN loc_jdr AS r ON l.a = r.a
) ORDER BY rn
SETTINGS enable_parallel_replicas = 0;
DROP TABLE loc_jdl;
DROP TABLE loc_jdr;

DROP TABLE dist_win;
DROP TABLE loc_win;
