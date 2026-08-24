-- Tags: shard

-- An `ALIAS` column defined over another `ALIAS` column has to be inlined all the way down to physical
-- columns before the query is shipped. Inlining one level leaves the intermediate alias in the shipped
-- SQL, and a shard that does not know it fails with
-- `Identifier '__table1.d' cannot be resolved from table with name __table1 (UNKNOWN_IDENTIFIER)`.
-- That is visible when the `ALIAS` columns exist only on the `Distributed` table, which is the only
-- place the chain can be resolved.
-- Carried over from https://github.com/ClickHouse/ClickHouse/pull/101789; the duplicate-`ALIAS`
-- half of that pull request is already covered on master.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS shard_chain_alias;
DROP TABLE IF EXISTS dist_chain_alias;

CREATE TABLE shard_chain_alias (a String, b Float64, c Float64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_chain_alias VALUES ('x', 1, 2);

-- `d`, `e` and `f` exist only here, so the shard can evaluate nothing but the fully inlined body.
CREATE TABLE dist_chain_alias (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS d, f Float64 ALIAS e + 1)
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_chain_alias, rand());

SELECT 'one link';
SELECT sum(e) FROM dist_chain_alias;

SELECT 'two links';
SELECT sum(f) FROM dist_chain_alias;

SELECT 'a chain next to its own base';
SELECT sum(d), sum(e) FROM dist_chain_alias;

SELECT 'the whole chain, plain and aggregated and filtered';
SELECT d, e, f FROM dist_chain_alias ORDER BY ALL;
SELECT sum(d), sum(e), sum(f) FROM dist_chain_alias WHERE e > 0 AND f > 0;

SELECT 'a chain whose body coincides with an explicit expression';
SELECT b + c + 1, d, e, f FROM dist_chain_alias ORDER BY ALL;

SELECT 'a chain used only in a clause';
SELECT a FROM dist_chain_alias WHERE f > 3 ORDER BY ALL;
SELECT a FROM dist_chain_alias GROUP BY a, e ORDER BY ALL;

DROP TABLE dist_chain_alias;
DROP TABLE shard_chain_alias;

-- The same chain declared on both tables, so the shard could resolve it itself: the result must not
-- depend on where the chain is declared.
DROP TABLE IF EXISTS shard_chain_alias_both;
DROP TABLE IF EXISTS dist_chain_alias_both;

CREATE TABLE shard_chain_alias_both (a String, b Float64, c Float64, d Float64 ALIAS b + c, e Float64 ALIAS d, f Float64 ALIAS e + 1)
    ENGINE = MergeTree() ORDER BY a;
INSERT INTO shard_chain_alias_both VALUES ('x', 1, 2);

CREATE TABLE dist_chain_alias_both AS shard_chain_alias_both
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), shard_chain_alias_both, rand());

SELECT 'declared on both tables';
SELECT sum(e) FROM dist_chain_alias_both;
SELECT sum(f) FROM dist_chain_alias_both;
SELECT d, e, f FROM dist_chain_alias_both ORDER BY ALL;

DROP TABLE dist_chain_alias_both;
DROP TABLE shard_chain_alias_both;
