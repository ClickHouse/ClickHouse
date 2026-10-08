-- Tags: long
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/74324
--
-- Sending an analyzed query tree to a remote replica via
-- `parallel_replicas_local_plan = 0` requires converting the tree back to AST.
-- The analyzer's expansion of an alias reference (e.g. `PREWHERE cond` where
-- `cond` is a projection alias) preserved inner aliases on the expanded body,
-- while the projection had the same body without those inner aliases.
-- The remote replica's analyzer then saw the same alias attached to two
-- different bodies and threw `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.
--
-- This test exercises the patterns reported in the issue plus the explicit
-- multi-occurrence projection cases highlighted by review:
--   1. Alias reused inside `PREWHERE` (`cond` references projection alias).
--   2. `SELECT *` over joined subqueries with overlapping column names.
--   3. Subcolumn rewriting under `optimize_functions_to_subcolumns`.
--   4. Explicit `r.name, l.name, l.name` over a 2-way join: the alias body
--      sequence here is `A, B, B`. The analyzer disambiguates the second and
--      third columns to a distinct alias (`l.name`) at AST conversion time,
--      so the dedup never collapses the body-B occurrences against body A.
--   5. `SELECT *` over a 3-way self-join with `USING`: the projection produces
--      the same bare alias (`name`, `value`) three times with bodies from
--      three different sources, exercising the dedup beyond the 2-occurrence
--      case from (2).

DROP TABLE IF EXISTS t_74324_prewhere;
DROP TABLE IF EXISTS t_74324_join;
DROP TABLE IF EXISTS t_74324_subcolumns;

-- 1. Alias reused inside PREWHERE.

CREATE TABLE t_74324_prewhere (id1 UInt64, id2 UInt64) ENGINE = MergeTree ORDER BY id1;
INSERT INTO t_74324_prewhere SELECT number, number FROM numbers(10);

SET enable_parallel_replicas = 1,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    enable_multiple_prewhere_read_steps = 1,
    move_all_conditions_to_prewhere = 1;

SELECT cast(id1 AS UInt16) AS cond1,
       (id2 % 40000)        AS cond2,
       (cond1 AND cond2)    AS cond
FROM t_74324_prewhere
PREWHERE cond
ORDER BY id1
LIMIT 10;

SELECT cast(id1 AS UInt16) AS cond1,
       (id2 % 40000)        AS cond2,
       (cond1 AND cond2)    AS cond
FROM t_74324_prewhere
PREWHERE cond1 AND id2 > 6 AND cond2
ORDER BY id1
LIMIT 10;

-- 2. SELECT * over a self-join with overlapping column names.

CREATE TABLE t_74324_join (id Int8, name String, value Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_74324_join VALUES (1, 'a', 10), (2, 'b', 20);

SET joined_subquery_requires_alias = 0;

SELECT *
FROM (SELECT * FROM t_74324_join) ANY LEFT JOIN (SELECT * FROM t_74324_join) USING id
WHERE id = 1
ORDER BY id;

-- 3. Subcolumn rewriting under analyzer + parallel replicas.

CREATE TABLE t_74324_subcolumns (id UInt64, n Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_74324_subcolumns VALUES (1, 'a'), (2, NULL);

SET optimize_functions_to_subcolumns = 1;

SELECT id, isNull(n) FROM t_74324_subcolumns ORDER BY id;

-- 4. Explicit `r.name, l.name, l.name` over a 2-way join.
--
-- The projection bodies are `[__table1.name, __table2.name, __table2.name]`
-- (the `A, B, B` pattern). The analyzer disambiguates the AST aliases to
-- `name, l.name, l.name`, so the dedup sees two independent alias groups
-- (`name` once, `l.name` twice with the same body) and leaves both `l.name`
-- aliases intact via the body-hash equality branch.

SET optimize_functions_to_subcolumns = 0;

SELECT r.name, l.name, l.name
FROM t_74324_join r ANY LEFT JOIN t_74324_join l ON r.id = l.id
WHERE r.id = 1
ORDER BY r.id;

-- 5. `SELECT *` over a 3-way self-join with `USING`.
--
-- The projection produces `[id, name, value, name, value, name, value]`.
-- The `name` alias appears at three positions with bodies from three
-- different sources (`A, B, C`); the dedup keeps the first occurrence and
-- strips the later two. `value` follows the same shape. Receiver-side
-- column lookup remains valid because positions 2-7 carry their body
-- expressions instead of a stale alias.

SELECT *
FROM (SELECT * FROM t_74324_join) ANY LEFT JOIN (SELECT * FROM t_74324_join) USING id ANY LEFT JOIN (SELECT * FROM t_74324_join) USING id
WHERE id = 1
ORDER BY id;

DROP TABLE t_74324_prewhere;
DROP TABLE t_74324_join;
DROP TABLE t_74324_subcolumns;
