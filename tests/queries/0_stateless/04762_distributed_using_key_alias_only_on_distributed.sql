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

DROP TABLE other_using_alias;
DROP TABLE dist_using_alias;
DROP TABLE shard_using_alias;
