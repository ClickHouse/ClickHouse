-- Tags: shard

DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_arr;

CREATE TABLE t_alias
(
    k UInt32,
    v Int64,
    s String,
    n Nullable(Int64),
    mat Int64 MATERIALIZED v * 10,
    a_v Int64 ALIAS v * 2,
    a_ls LowCardinality(String) ALIAS s,
    a_null Nullable(Int64) ALIAS n + 1,
    a_cast Int32 ALIAS v + 1,
    a_on_a Int64 ALIAS a_v + 1,
    a_mat Int64 ALIAS mat + 1,
    a_inner Int64 ALIAS ((v + 1) AS inside_value) + inside_value,
    a_dup1 String ALIAS toString(v),
    a_dup2 String ALIAS toString(v)
)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_plain (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_arr (k UInt32, arr Array(Int64), a_k Int64 ALIAS k * 3) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_alias SELECT number, number, toString(number % 7), if(number % 3 = 0, NULL, number) FROM numbers(100);
INSERT INTO t_plain SELECT number * 2, number FROM numbers(50);
INSERT INTO t_arr SELECT number, [number, number + 1] FROM numbers(40);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Pin what the runner randomizes, otherwise the queries silently move off the path under test:
-- `automatic_parallel_replicas_mode` declines parallel replicas for a table this small, and
-- `parallel_replicas_local_plan` swaps the local-plan path for the AST one.
SET automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

SELECT 'local plan';

SELECT r.a_v FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.a_ls = r.a_ls ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v FROM t_plain AS p GLOBAL INNER JOIN t_alias AS l ON p.k = l.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_null, toTypeName(r.a_null) FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC NULLS LAST LIMIT 3;
SELECT r.a_cast, toTypeName(r.a_cast) FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_ls, toTypeName(r.a_ls) FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_on_a FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_mat FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_inner FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_dup1, r.a_dup2 FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v, r.a_v FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_v AS user_name FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY user_name DESC LIMIT 3;
SELECT r.a_ls, count() FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k GROUP BY r.a_ls ORDER BY ALL LIMIT 3;
SELECT r.k FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY r.a_v DESC, r.k DESC LIMIT 3;
SELECT sum(r.a_v) FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k;
SELECT l.a_v FROM t_alias AS l GLOBAL RIGHT JOIN t_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_v FROM t_arr AS x ARRAY JOIN x.arr GLOBAL INNER JOIN t_alias AS r ON x.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT x.a_k FROM t_arr AS x ARRAY JOIN x.arr GLOBAL RIGHT JOIN t_plain AS p ON x.k = p.k ORDER BY ALL DESC LIMIT 3;
SELECT count() FROM t_plain WHERE k GLOBAL IN (SELECT k FROM t_alias WHERE a_v > 20);
SELECT k FROM t_alias GLOBAL INNER JOIN t_plain USING (k) ORDER BY ALL DESC LIMIT 3;

SELECT 'nested subquery';

-- Parallel replicas pushed down into a subquery goes through the other shipping caller
-- (buildQueryPlanForParallelReplicas).
SELECT z FROM (SELECT r.a_v AS z FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k) ORDER BY ALL DESC LIMIT 3;
SELECT z FROM (SELECT r.a_ls AS z FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k) ORDER BY ALL DESC LIMIT 3;
SELECT count() FROM (SELECT r.k FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k WHERE r.a_v > 10);
SELECT sum(z) FROM (SELECT r.a_v AS z FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k);

SELECT 'prefer local join off';

SET parallel_replicas_prefer_local_join = 0;
SELECT l.a_v FROM t_alias AS l RIGHT ANTI JOIN t_plain AS p ON l.k = p.k ORDER BY ALL LIMIT 3;
SELECT l.a_v FROM t_alias AS l RIGHT SEMI JOIN t_plain AS p ON l.k = p.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_v FROM t_plain AS p INNER JOIN t_alias AS r ON p.k = r.k ORDER BY ALL DESC LIMIT 3;
SET parallel_replicas_prefer_local_join = 1;

SELECT 'ast dispatch';

SET parallel_replicas_local_plan = 0;
SELECT r.a_v FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT l.a_v, r.a_v FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_cast, toTypeName(r.a_cast) FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_dup1, r.a_dup2 FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k ORDER BY ALL DESC LIMIT 3;
SELECT r.a_ls, count() FROM t_alias AS l GLOBAL INNER JOIN t_alias AS r ON l.k = r.k GROUP BY r.a_ls ORDER BY ALL LIMIT 3;
SELECT r.a_v FROM t_arr AS x ARRAY JOIN x.arr GLOBAL INNER JOIN t_alias AS r ON x.k = r.k ORDER BY ALL DESC LIMIT 3;

DROP TABLE t_arr;
DROP TABLE t_plain;
DROP TABLE t_alias;
