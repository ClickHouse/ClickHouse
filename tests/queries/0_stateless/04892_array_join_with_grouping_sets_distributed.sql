-- `CreateUniqueArrayJoinAliasesVisitor` renames `ARRAY JOIN` expressions when building the query tree
-- for a shard. It walks over every column node, and the virtual `__grouping_set` column added for
-- `GROUPING SETS`/`ROLLUP`/`CUBE` has no source by design, so the visitor must tolerate a sourceless
-- column while still renaming the array-joined ones.

DROP TABLE IF EXISTS t_array_join_grouping_sets;

CREATE TABLE t_array_join_grouping_sets (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_join_grouping_sets VALUES (1, [10, 20]), (2, [20, 30]);

SELECT id, elem, grouping(id) AS g_id, grouping(elem) AS g_elem, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_join_grouping_sets)
ARRAY JOIN arr AS elem
GROUP BY GROUPING SETS ((id), (elem))
ORDER BY id, elem;

SELECT '--- ROLLUP ---';

SELECT id, elem, grouping(id, elem) AS g, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_join_grouping_sets)
ARRAY JOIN arr AS elem
GROUP BY ROLLUP(id, elem)
ORDER BY id, elem;

SELECT '--- CUBE ---';

SELECT id, elem, grouping(id, elem) AS g, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_join_grouping_sets)
ARRAY JOIN arr AS elem
GROUP BY CUBE(id, elem)
ORDER BY id, elem;

DROP TABLE t_array_join_grouping_sets;
