-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/85895
-- When several ALIAS columns share the same expression, the Distributed engine
-- must not collapse them into a single column of the block sent over the
-- network. At the same time, referencing the *same* ALIAS column from more
-- than one clause (SELECT + GROUP BY / HAVING / ORDER BY) must keep a single
-- expansion per alias, otherwise the analyzer raises
-- MULTIPLE_EXPRESSIONS_FOR_ALIAS.

DROP TABLE IF EXISTS shard_dup_alias;
DROP TABLE IF EXISTS dist_dup_alias;

CREATE TABLE shard_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c, f Float64 ALIAS b + c) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_dup_alias VALUES ('x', 1, 2);

CREATE TABLE dist_dup_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c, f Float64 ALIAS b + c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_dup_alias, rand());

-- The original bug: two ALIAS columns with the same expression.
SELECT sum(d) AS x, sum(e) AS y FROM dist_dup_alias;

-- More than two ALIAS columns with the same expression.
SELECT sum(d), sum(e), sum(f) FROM dist_dup_alias;

-- Plain selection of duplicate aliases, no aggregation.
SELECT d, e FROM dist_dup_alias;

-- Duplicate aliases as GROUP BY keys.
SELECT d, e, count() FROM dist_dup_alias GROUP BY d, e;

-- Same ALIAS column referenced twice: SELECT + GROUP BY.
SELECT d, count() FROM dist_dup_alias GROUP BY d ORDER BY d;

-- Same ALIAS column referenced three times: SELECT + GROUP BY + HAVING.
SELECT d, count() FROM dist_dup_alias GROUP BY d HAVING d > 0 ORDER BY d;

-- Same ALIAS column referenced in SELECT + ORDER BY.
SELECT d FROM dist_dup_alias ORDER BY d;

-- Different duplicate aliases referenced multiple times each.
SELECT d, e, d + e FROM dist_dup_alias ORDER BY d, e;

-- Duplicate aliases in both SELECT and WHERE.
SELECT sum(d), sum(e) FROM dist_dup_alias WHERE e > 0 AND d < 100;

-- Duplicate aliases in a subquery.
SELECT sum(d), sum(e) FROM (SELECT d, e FROM dist_dup_alias);

-- Two table expressions of one query, each with its own duplicate aliases.
SELECT sum(t1.d), sum(t2.e) FROM dist_dup_alias t1 GLOBAL JOIN dist_dup_alias t2 ON t1.a = t2.a;

-- ALIAS columns defined only on the Distributed table, missing on the shard
-- table: the expansion is the only thing the shards can evaluate.
DROP TABLE IF EXISTS shard_no_alias;
DROP TABLE IF EXISTS dist_only_alias;

CREATE TABLE shard_no_alias (a String, b Float64, c Float64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_no_alias VALUES ('x', 3, 4);

CREATE TABLE dist_only_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS b + c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_no_alias, rand());

SELECT sum(d), sum(e) FROM dist_only_alias;

-- Multiple sources with the same alias names but different expressions:
-- the duplicate detection must be scoped per table, so the unique `d` of one
-- source must not influence the duplicate `d`, `e` of the other.
DROP TABLE IF EXISTS shard_other_alias;
DROP TABLE IF EXISTS dist_other_alias;

CREATE TABLE shard_other_alias (a String, b Float64, c Float64, d Float64 ALIAS b - c, e Float64 ALIAS b - c) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_other_alias VALUES ('x', 10, 1);

CREATE TABLE dist_other_alias (a String, b Float64, c Float64, d Float64 ALIAS b - c, e Float64 ALIAS b - c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_other_alias, rand());

SELECT sum(d), sum(e) FROM (SELECT d, e FROM dist_dup_alias UNION ALL SELECT d, e FROM dist_other_alias);

-- The bare expansion in WHERE must not interfere with primary key analysis
-- on the shards: with the key (b + c) and the condition on its alias, only a
-- couple of granules may be read, not the whole table.
DROP TABLE IF EXISTS shard_pk_alias;
DROP TABLE IF EXISTS dist_pk_alias;

CREATE TABLE shard_pk_alias (b UInt32, c UInt32, d UInt32 ALIAS b + c, e UInt32 ALIAS b + c)
    ENGINE = MergeTree() ORDER BY (b + c) SETTINGS index_granularity = 8192;
INSERT INTO shard_pk_alias (b, c) SELECT number, number FROM numbers(1000000);

CREATE TABLE dist_pk_alias (b UInt32, c UInt32, d UInt32 ALIAS b + c, e UInt32 ALIAS b + c)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_pk_alias, rand());

SELECT sum(d), sum(e) FROM dist_pk_alias WHERE e = 10 SETTINGS log_comment = '04090_alias_pk_prune';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 500000 FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04090_alias_pk_prune' AND type = 'QueryFinish' AND is_initial_query
    ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE dist_pk_alias;
DROP TABLE shard_pk_alias;
DROP TABLE dist_other_alias;
DROP TABLE shard_other_alias;
DROP TABLE dist_only_alias;
DROP TABLE shard_no_alias;
DROP TABLE dist_dup_alias;
DROP TABLE shard_dup_alias;
