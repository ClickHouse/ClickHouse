-- Tags: distributed

-- Regression for the finalize-visitor passthrough branch.
-- ARRAY JOIN sources do not receive aliases from createUniqueAliasesIfNecessary, so a
-- __aliasMarker(elem, elem) over an ARRAY JOIN'd variable previously hit the
-- "unaliased source" path and threw LOGICAL_ERROR. Behaviour must be identity passthrough
-- on both local and distributed paths.

DROP TABLE IF EXISTS t_local_04285;
DROP TABLE IF EXISTS t_dist_04285;

CREATE TABLE t_local_04285 (arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_local_04285 VALUES ([1, 2, 3]);

CREATE TABLE t_dist_04285 AS t_local_04285
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local_04285);

SELECT 'local';
SELECT __aliasMarker(elem, elem) FROM t_local_04285 ARRAY JOIN arr AS elem ORDER BY elem
SETTINGS enable_alias_marker = 1, send_logs_level = 'error';

SELECT 'distributed';
SELECT __aliasMarker(elem, elem) FROM t_dist_04285 ARRAY JOIN arr AS elem ORDER BY elem
SETTINGS enable_alias_marker = 1, send_logs_level = 'error';

DROP TABLE t_dist_04285;
DROP TABLE t_local_04285;
