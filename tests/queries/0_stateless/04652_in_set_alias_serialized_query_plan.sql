-- Tags: shard

-- A `Set`-engine table on the right of IN is consumed natively as a prepared set. When the query
-- plan is serialized to the shards, the set is rebuilt from its table name on the receiving side,
-- which has to recognize the set-backed table through any `Alias` wrapping. Without unwrapping,
-- `x IN alias_to_set` threw `INCORRECT_DATA: Table ... is not a StorageSet` there while the same
-- query worked with `serialize_query_plan = 0`.

DROP TABLE IF EXISTS t_04652_set;
DROP TABLE IF EXISTS t_04652_set_alias;
DROP TABLE IF EXISTS t_04652_src;

CREATE TABLE t_04652_set (arr Array(UInt8)) ENGINE = Set;
INSERT INTO t_04652_set VALUES ([1, 2, 3]), ([4, 5]);
CREATE TABLE t_04652_set_alias ENGINE = Alias('t_04652_set');

CREATE TABLE t_04652_src (a Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04652_src VALUES ([1, 2, 3]), ([9, 9]);

-- Two shards, so the plan is really serialized and the sets are rebuilt on the receiving side.
-- `enable_analyzer` and `prefer_localhost_replica` are pinned because the plan is only serialized on
-- the analyzer's remote path, and the runner randomizes both.
-- The alias must give the same result as the direct `Set` table, with and without serialization.
SELECT count() FROM cluster('test_cluster_two_shards', currentDatabase(), t_04652_src) WHERE a IN t_04652_set SETTINGS serialize_query_plan = 1, enable_analyzer = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_cluster_two_shards', currentDatabase(), t_04652_src) WHERE a IN t_04652_set_alias SETTINGS serialize_query_plan = 1, enable_analyzer = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_cluster_two_shards', currentDatabase(), t_04652_src) WHERE a IN t_04652_set_alias SETTINGS serialize_query_plan = 0, enable_analyzer = 1, prefer_localhost_replica = 0;

DROP TABLE t_04652_src;
DROP TABLE t_04652_set_alias;
DROP TABLE t_04652_set;
