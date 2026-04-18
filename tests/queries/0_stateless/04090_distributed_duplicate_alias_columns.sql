-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/85895
-- When two ALIAS columns share the same expression, the Distributed engine
-- must not deduplicate them into a single aggregate column. At the same time,
-- referencing the *same* ALIAS column from more than one clause (SELECT +
-- GROUP BY / HAVING / ORDER BY) must keep a single expansion per alias —
-- otherwise the analyzer raises MULTIPLE_EXPRESSIONS_FOR_ALIAS.

DROP TABLE IF EXISTS shard_dup_alias;
DROP TABLE IF EXISTS dist_dup_alias;

CREATE TABLE shard_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_dup_alias VALUES ('x', 1, 2);

CREATE TABLE dist_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_dup_alias, rand());

-- Original bug: two ALIAS columns with the same expression.
SELECT sum(d) AS f, sum(e) AS g FROM dist_dup_alias;

-- Plain selection of both aliases.
SELECT d, e FROM dist_dup_alias;

-- Same ALIAS column referenced twice: SELECT + GROUP BY.  Before the fix this
-- expanded once as `b + c AS d` and once as `identity(b + c AS d) AS d`, and
-- the analyzer raised MULTIPLE_EXPRESSIONS_FOR_ALIAS. Must return the grouped value.
SELECT d, count() FROM dist_dup_alias GROUP BY d ORDER BY d;

-- Same ALIAS column referenced three times: SELECT + GROUP BY + HAVING.
SELECT d, count() FROM dist_dup_alias GROUP BY d HAVING d > 0 ORDER BY d;

-- Same ALIAS column referenced in SELECT + ORDER BY.
SELECT d FROM dist_dup_alias ORDER BY d;

-- Different aliases (`d` and `e`) referenced multiple times each. The second
-- alias gets wrapped in `identity`; every visit of the same alias must keep
-- using the same expansion.
SELECT d, e, d + e FROM dist_dup_alias ORDER BY d, e;

DROP TABLE dist_dup_alias;
DROP TABLE shard_dup_alias;
