-- Tags: shard

-- An `ALIAS` column of the table expression that a query ships to its replicas used to fail with
-- `NO_SUCH_COLUMN_IN_TABLE`: the shipped subquery is rebuilt from column names and types only, which
-- drops the alias body, so the remote side asked storage for a column it does not have. The
-- Distributed path was unaffected because it inlines `ALIAS` columns before shipping; the
-- parallel-replicas paths did not.
-- Inlining must not stamp the column's name on the inlined body outside the projection, or two `JOIN`
-- sides declaring a same-named `ALIAS` column put one alias on two bodies and the remote side throws
-- `MULTIPLE_EXPRESSIONS_FOR_ALIAS` instead.

-- The shipping path under test only exists in the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_ship_alias;
DROP TABLE IF EXISTS t_ship_plain;
DROP TABLE IF EXISTS t_ship_other;

CREATE TABLE t_ship_alias
(
    k UInt32,
    v Int64,
    s String,
    n Nullable(Int64),
    mat Int64 MATERIALIZED v * 10,
    a_v Int64 ALIAS v * 2,
    a_lc LowCardinality(String) ALIAS s,
    a_null Nullable(Int64) ALIAS n + 1,
    a_narrow Int32 ALIAS v + 1,
    a_on_a Int64 ALIAS a_v + 1,
    a_mat Int64 ALIAS mat + 1,
    a_common Int64 ALIAS v + 1,
    -- Two aliases with identical bodies: they collapse to one column on the remote side.
    a_dup1 String ALIAS toString(v),
    a_dup2 String ALIAS toString(v)
)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_ship_plain (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k;
-- Declares `a_common` too, with a different body, to cover the same-name-on-both-sides case.
CREATE TABLE t_ship_other (k UInt32, w Int64, a_common Int64 ALIAS w + 2) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_ship_arr (k UInt32, arr Array(Int64), a_k Int64 ALIAS k * 3) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_ship_alias SELECT number, number, toString(number % 7), if(number % 3 = 0, NULL, number) FROM numbers(30);
INSERT INTO t_ship_plain SELECT number * 2, number FROM numbers(15);
INSERT INTO t_ship_other SELECT number, number FROM numbers(30);
INSERT INTO t_ship_arr SELECT number, [number, number + 1] FROM numbers(20);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Pin what the test runner randomizes, otherwise these queries silently move off the path under test:
-- `automatic_parallel_replicas_mode` declines parallel replicas for a table this small and
-- `parallel_replicas_local_plan` selects between the two shipping callers.
SET automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

SELECT 'shipped side owns the alias';

SELECT r.a_v FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v FROM t_ship_plain AS p GLOBAL INNER JOIN t_ship_alias AS l ON p.k = l.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_null, toTypeName(r.a_null) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC NULLS LAST LIMIT 3;
SELECT r.a_lc, toTypeName(r.a_lc) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
-- Declared type narrower than the body's, so the stored body carries a cast that must survive inlining.
SELECT r.a_narrow, toTypeName(r.a_narrow) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_on_a FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_mat FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'alias only in a clause, not in the projection';

SELECT r.k FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY r.a_v DESC, r.k DESC LIMIT 3;
SELECT r.a_lc, count() FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k GROUP BY r.a_lc ORDER BY ALL LIMIT 3;
SELECT sum(r.a_v) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k;
SELECT count() FROM t_ship_plain WHERE k GLOBAL IN (SELECT k FROM t_ship_alias WHERE a_v > 20);

SELECT 'same-named ALIAS column on both sides';

SELECT l.a_common, r.a_common FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_other AS r ON l.k = r.k ORDER BY ALL LIMIT 3;
SELECT l.a_v, r.a_v FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'two aliases with the same body';

SELECT r.a_dup1, r.a_dup2 FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'shipped through ARRAY JOIN and USING';

SELECT r.a_v FROM t_ship_arr AS x ARRAY JOIN x.arr GLOBAL INNER JOIN t_ship_alias AS r ON x.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT x.a_k FROM t_ship_arr AS x ARRAY JOIN x.arr GLOBAL RIGHT JOIN t_ship_plain AS p ON x.k = p.k ORDER BY ALL DESC LIMIT 3;
-- A `JOIN USING` key list records how the key resolves per side and must survive inlining untouched.
SELECT k FROM t_ship_alias GLOBAL INNER JOIN t_ship_plain USING (k) ORDER BY ALL DESC LIMIT 3;
SELECT a_v FROM t_ship_alias GLOBAL INNER JOIN t_ship_plain USING (k) ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v FROM t_ship_alias AS l GLOBAL RIGHT JOIN t_ship_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;

SELECT 'parallel replicas pushed into a subquery';

-- Goes through the other shipping caller, buildQueryPlanForParallelReplicas.
SELECT z FROM (SELECT r.a_v AS z FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k) ORDER BY ALL DESC LIMIT 3;
SELECT sum(z) FROM (SELECT r.a_v AS z FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k);

SELECT 'right-side join kinds with a local join declined';

SET parallel_replicas_prefer_local_join = 0;
SELECT l.a_v FROM t_ship_alias AS l RIGHT ANTI JOIN t_ship_plain AS p ON l.k = p.k ORDER BY ALL LIMIT 3;
SELECT l.a_v FROM t_ship_alias AS l RIGHT SEMI JOIN t_ship_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;
SET parallel_replicas_prefer_local_join = 1;

SELECT 'query shipped as SQL instead of a plan';

SET parallel_replicas_local_plan = 0;
SELECT r.a_v FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT l.a_common, r.a_common FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_other AS r ON l.k = r.k ORDER BY ALL LIMIT 3;
SELECT r.a_narrow, toTypeName(r.a_narrow) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'same-named ALIAS column on both sides, shipped by remote()';

-- The same shape on the Distributed path, which has always inlined and so has always put one alias on
-- two different bodies here.
SET enable_parallel_replicas = 0;
SELECT l.a_common, r.a_common
FROM remote('127.0.0.1', currentDatabase(), t_ship_alias) AS l
GLOBAL INNER JOIN remote('127.0.0.1', currentDatabase(), t_ship_other) AS r ON l.k = r.k
ORDER BY ALL LIMIT 3;

SELECT l.a_common FROM remote('127.0.0.1', currentDatabase(), t_ship_alias) AS l
GLOBAL INNER JOIN remote('127.0.0.1', currentDatabase(), t_ship_other) AS r ON l.k = r.k
WHERE r.a_common > 2 ORDER BY ALL LIMIT 3;

DROP TABLE t_ship_arr;
DROP TABLE t_ship_other;
DROP TABLE t_ship_plain;
DROP TABLE t_ship_alias;
