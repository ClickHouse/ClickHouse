-- Tags: shard

-- An `ALIAS` column of the table expression that a query ships to its replicas used to fail with
-- `NO_SUCH_COLUMN_IN_TABLE`: the shipped subquery is rebuilt from column names and types only, which
-- drops the alias body, so the remote side asked storage for a column it does not have. The
-- Distributed path was unaffected because it inlines `ALIAS` columns before shipping; the
-- parallel-replicas paths did not.
-- The join shapes and dispatch modes are in 04761, to keep each test's runtime down.

-- The shipping path under test only exists in the analyzer.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_ship_alias;
DROP TABLE IF EXISTS t_ship_plain;

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
    -- Two aliases with identical bodies: they collapse to one column on the remote side.
    a_dup1 String ALIAS toString(v),
    a_dup2 String ALIAS toString(v),
    -- A volatile body: every reference to one `ALIAS` column must still see one value per row.
    a_rand UInt32 ALIAS rand()
)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_ship_plain (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_ship_alias SELECT number, number, toString(number % 7), if(number % 3 = 0, NULL, number) FROM numbers(30);
INSERT INTO t_ship_plain SELECT number * 2, number FROM numbers(15);

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

SELECT 'two aliases with the same body';

SELECT r.a_dup1, r.a_dup2 FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;

SELECT 'alias only in a clause, not in the projection';

SELECT r.k FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k ORDER BY r.a_v DESC, r.k DESC LIMIT 3;
SELECT r.a_lc, count() FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k GROUP BY r.a_lc ORDER BY ALL LIMIT 3;
SELECT sum(r.a_v) FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k;
SELECT count() FROM t_ship_plain WHERE k GLOBAL IN (SELECT k FROM t_ship_alias WHERE a_v > 20);

SELECT 'a volatile alias body is still one value per row';

-- Inlining gives each reference its own copy of the body, so a volatile body would be drawn twice if the
-- copies did not converge again on the remote side. They do: an action node is named after the expression
-- rather than the SQL alias, so structurally identical copies share one node. Both answers are 0 whatever
-- `rand()` returns, and would be about half the rows if the references came apart.
SELECT countIf(p <> q) FROM (SELECT r.a_rand AS p, r.a_rand AS q FROM t_ship_plain AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k);
SELECT countIf(a_rand % 2 != 0) FROM (SELECT r.a_rand AS a_rand FROM t_ship_plain AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k WHERE r.a_rand % 2 = 0);

SELECT 'parallel replicas pushed into a subquery';

-- Goes through the other shipping caller, buildQueryPlanForParallelReplicas.
SELECT z FROM (SELECT r.a_v AS z FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k) ORDER BY ALL DESC LIMIT 3;
SELECT sum(z) FROM (SELECT r.a_v AS z FROM t_ship_alias AS l GLOBAL INNER JOIN t_ship_alias AS r ON l.k = r.k);

DROP TABLE t_ship_plain;
DROP TABLE t_ship_alias;
