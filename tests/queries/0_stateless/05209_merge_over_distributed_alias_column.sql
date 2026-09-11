-- An `ALIAS` column whose expression is a bare reference to another column read back as the default
-- value of its type through a `Merge` table over a `Distributed` table with a remote shard. The whole
-- query is delegated to the shards there, and `Distributed` inlines the `ALIAS` column into its
-- expression before sending it, so `b UInt64 ALIAS a` selected next to `a` came back as one column,
-- and `Merge` filled the column it was still expecting with the default value instead.

DROP TABLE IF EXISTS t_05209;
DROP TABLE IF EXISTS t_05209_dist;
DROP TABLE IF EXISTS t_05209_merge;

CREATE TABLE t_05209 (a UInt64, b UInt64 ALIAS a, c UInt64 ALIAS b, d UInt8 ALIAS a > 0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05209 SELECT number + 1 FROM numbers(4);

CREATE TABLE t_05209_dist AS t_05209 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_05209);
CREATE TABLE t_05209_merge AS t_05209 ENGINE = Merge(currentDatabase(), '^t_05209_dist$');

SET enable_analyzer = 1;

SELECT 'distributed', a, b, c, d FROM t_05209_dist ORDER BY a LIMIT 2;
SELECT 'merge', a, b, c, d FROM t_05209_merge ORDER BY a LIMIT 2;
SELECT 'table function', a, b FROM merge(currentDatabase(), '^t_05209_dist$') ORDER BY a LIMIT 2;
SELECT 'filter on the alias', a, b FROM t_05209_merge WHERE b = 1 ORDER BY a LIMIT 2;
SELECT 'sums', sum(a), sum(b), sum(c), sum(d) FROM t_05209_merge;
SELECT 'the alias alone', b FROM t_05209_merge ORDER BY b LIMIT 2;
SELECT 'group by the alias', b, count() FROM t_05209_merge GROUP BY b ORDER BY b;
SELECT 'order by the alias', a, b FROM t_05209_merge ORDER BY b DESC, a LIMIT 2;
SELECT 'the function alias alone', a, d FROM t_05209_merge ORDER BY a LIMIT 2;
SELECT 'merge over the local table', a, b, c, d FROM merge(currentDatabase(), '^t_05209$') ORDER BY a LIMIT 2;
SELECT 'old analyzer', a, b, c, d FROM t_05209_merge ORDER BY a LIMIT 2 SETTINGS enable_analyzer = 0;
