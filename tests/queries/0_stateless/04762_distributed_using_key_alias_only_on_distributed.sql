-- Tags: shard

-- A `JOIN USING` key that is an `ALIAS` column declared only on the `Distributed` table can be resolved
-- by the shard only if the shipped SQL carries the alias body: `USING (x AS a)` rather than `USING (a)`,
-- because the shard's table has `x` and has never heard of `a`.
-- The initiator cannot tell the difference on its own: `buildQueryTreeDistributed` replaces the source
-- with a dummy built from the `Distributed` table's own columns, so the key does look resolvable there.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS shard_using_alias;
DROP TABLE IF EXISTS dist_using_alias;
DROP TABLE IF EXISTS other_using_alias;

-- The shard table has neither `a` nor `chained`; the `Distributed` table declares both as `ALIAS`.
CREATE TABLE shard_using_alias (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_using_alias SELECT number, number * 10 FROM numbers(5);

CREATE TABLE dist_using_alias (x UInt32, y UInt32, a UInt32 ALIAS x, chained UInt32 ALIAS a)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_using_alias, rand());

CREATE TABLE other_using_alias (a UInt32, chained UInt32, tag String) ENGINE = MergeTree ORDER BY a;
INSERT INTO other_using_alias SELECT number, number, 't' FROM numbers(5);

SELECT 'the key is an ALIAS column of the Distributed table';
SELECT a FROM dist_using_alias JOIN other_using_alias USING (a) ORDER BY ALL;

SELECT 'other columns alongside the key';
SELECT a, tag, y FROM dist_using_alias JOIN other_using_alias USING (a) ORDER BY ALL;

SELECT 'the Distributed table on the right of the join';
SELECT a FROM other_using_alias JOIN dist_using_alias USING (a) ORDER BY ALL;

SELECT 'with the top-level identifier compatibility setting';
SELECT a FROM dist_using_alias JOIN other_using_alias USING (a) ORDER BY ALL
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'the key is a chain of ALIAS columns';
SELECT chained FROM dist_using_alias JOIN other_using_alias USING (chained) ORDER BY ALL;

SELECT 'the key exists on both tables as a real column';
SELECT x FROM dist_using_alias JOIN (SELECT number AS x FROM numbers(5)) AS n USING (x) ORDER BY ALL;

-- An alias whose body is an expression rather than a bare column cannot travel in the `USING` clause at
-- all: that clause takes identifiers, so `USING (x + 1 AS a)` is not something a query can even say, and
-- the shard receives `USING (a)` and cannot resolve it. This is a limitation of the clause rather than of
-- shipping, and it is the same on a plain `MergeTree` table. `analyzer_compatibility_join_using_top_level_identifier`
-- is the way through: the shard then resolves the key from the alias in the `SELECT` list, which is the
-- same expression the initiator meant, so the result is correct.
DROP TABLE IF EXISTS shard_expr_key;
DROP TABLE IF EXISTS dist_expr_key;
DROP TABLE IF EXISTS other_expr_key;

CREATE TABLE shard_expr_key (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_expr_key SELECT number FROM numbers(5);
CREATE TABLE dist_expr_key (x UInt32, a UInt32 ALIAS x + 1)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_expr_key, rand());
CREATE TABLE other_expr_key (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO other_expr_key SELECT number FROM numbers(6);

SELECT 'the key is an ALIAS column whose body is an expression';
SELECT a FROM dist_expr_key JOIN other_expr_key USING (a) ORDER BY ALL; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'the same, resolved through the SELECT list';
SELECT a FROM dist_expr_key JOIN other_expr_key USING (a) ORDER BY ALL
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

DROP TABLE other_expr_key;
DROP TABLE dist_expr_key;
DROP TABLE shard_expr_key;

-- The initiator and the shard disagree when a `USING` key resolves to an alias nested in the `SELECT`
-- list while the left table also has a real column of that name. Only top-level aliases survive into the
-- shipped SQL, so the shard cannot see the nested one and silently joins by the real column instead.
-- The key is shipped rather than rejected by an explicit owner decision, recorded at
-- https://github.com/ClickHouse/ClickHouse/pull/110739#discussion_r3629326104, which reads in part:
-- "the remote server joins by the real column, so the result may differ from local execution - this
-- divergence is accepted, documented in the setting description, and pinned by a test case".
-- https://github.com/ClickHouse/ClickHouse/issues/111276 stays open to document it.
-- Pinned here as well, on the values, so that a change of mind on that decision shows up as a diff:
-- the local answer is 11 and the distributed one is 0 for the same data.
DROP TABLE IF EXISTS shard_shadowed_key;
DROP TABLE IF EXISTS dist_shadowed_key;

CREATE TABLE shard_shadowed_key (x UInt64, id UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_shadowed_key VALUES (1, 5);

CREATE TABLE dist_shadowed_key AS shard_shadowed_key
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_shadowed_key, rand());

SELECT 'a nested SELECT-list alias shadowing a real column, locally';
SELECT sum(x + 10 AS id) FROM shard_shadowed_key AS t JOIN (SELECT 11 AS id) t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

SELECT 'the same over a Distributed table: joins by the real column instead';
SELECT sum(x + 10 AS id) FROM dist_shadowed_key AS t JOIN (SELECT 11 AS id) t2 USING (id)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1, distributed_product_mode = 'local';

DROP TABLE dist_shadowed_key;
DROP TABLE shard_shadowed_key;

DROP TABLE other_using_alias;
DROP TABLE dist_using_alias;
DROP TABLE shard_using_alias;
