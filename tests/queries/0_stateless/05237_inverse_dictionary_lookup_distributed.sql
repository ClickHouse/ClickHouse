-- Tags: distributed

-- `optimize_inverse_dictionary_lookup` inserts `tupleElement` (unwrapping a single-column complex key)
-- and `accurateCast` (converting a signed simple-key probe to `UInt64`) into the query tree. A remote
-- shard re-analyzes the rewritten query, so the results must be the same with the optimization on and
-- off also for a `Distributed` table, whether the local shard is queried directly or over the network.
-- The first case is the reproducer of https://github.com/ClickHouse/ClickHouse/issues/121913.

SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 0;

DROP TABLE IF EXISTS t1_dist;
DROP TABLE IF EXISTS t1;
DROP DICTIONARY IF EXISTS dict_uuid;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS signed_dist;
DROP TABLE IF EXISTS signed_probes;
DROP DICTIONARY IF EXISTS dict_signed;
DROP TABLE IF EXISTS signed_src;

CREATE TABLE t_src (id UUID, c2 String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src SELECT generateUUIDv4(), if(number % 2 = 0, 'a', 'b') FROM numbers(100);

CREATE TABLE t1 (c1 UUID, kt Tuple(UUID)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t1 SELECT id, tuple(id) FROM t_src;

CREATE DICTIONARY dict_uuid (id UUID, c2 String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't_src')) LIFETIME(0) LAYOUT(COMPLEX_KEY_HASHED());

CREATE TABLE t1_dist AS t1 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t1);

CREATE TABLE signed_src (id Int64, attr String) ENGINE = MergeTree ORDER BY id;
INSERT INTO signed_src VALUES (1, 'alpha'), (2, 'beta'), (3, 'beta');

CREATE DICTIONARY dict_signed (id Int64, attr String DEFAULT '')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'signed_src')) LIFETIME(0) LAYOUT(FLAT());

CREATE TABLE signed_probes (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO signed_probes VALUES (1), (2), (4);

CREATE TABLE signed_dist AS signed_probes ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), signed_probes);

SELECT 'issue 121913, local';
SELECT count() FROM t1 WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) = 'a';
SELECT count() FROM t1 WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT 'tuple(c1), equals, distributed';
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 1;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 0, prefer_localhost_replica = 0;

SELECT 'tuple(c1), like, distributed';
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) LIKE 'a%'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) LIKE 'a%'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 1;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(c1)) LIKE 'a%'
SETTINGS optimize_inverse_dictionary_lookup = 0, prefer_localhost_replica = 0;

-- A `Tuple(UUID)` column is unwrapped with `tupleElement`.
SELECT 'tuple-typed column, distributed';
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', kt) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', kt) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 1;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', kt) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 0, prefer_localhost_replica = 0;

-- A signed probe of a simple key is converted with `accurateCast`, in `WHERE` and in the projection.
SELECT 'signed simple key, distributed';
SELECT id, dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) = 'beta' AS p FROM signed_dist
WHERE dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) LIKE '%a' ORDER BY id, p
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;
SELECT id, dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) = 'beta' AS p FROM signed_dist
WHERE dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) LIKE '%a' ORDER BY id, p
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 1;
SELECT id, dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) = 'beta' AS p FROM signed_dist
WHERE dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', id) LIKE '%a' ORDER BY id, p
SETTINGS optimize_inverse_dictionary_lookup = 0, prefer_localhost_replica = 0;

-- Constant and materialized constant probes: the resolver folds the former before the pass runs,
-- the latter is rewritten and must not produce a different header on the shard.
SELECT 'constant probes, distributed';
SELECT dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', toInt64(1)) = 'alpha' AS c,
    dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', materialize(toInt64(2))) = 'beta' AS m
FROM signed_dist ORDER BY c, m
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;
SELECT dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', toInt64(1)) = 'alpha' AS c,
    dictGet(concat(currentDatabase(), '.dict_signed'), 'attr', materialize(toInt64(2))) = 'beta' AS m
FROM signed_dist ORDER BY c, m
SETTINGS optimize_inverse_dictionary_lookup = 0, prefer_localhost_replica = 0;
SELECT count() FROM t1_dist WHERE dictGet(concat(currentDatabase(), '.dict_uuid'), 'c2', tuple(materialize(c1))) = 'a'
SETTINGS optimize_inverse_dictionary_lookup = 1, prefer_localhost_replica = 0;

DROP TABLE t1_dist;
DROP TABLE t1;
DROP DICTIONARY dict_uuid;
DROP TABLE t_src;
DROP TABLE signed_dist;
DROP TABLE signed_probes;
DROP DICTIONARY dict_signed;
DROP TABLE signed_src;
