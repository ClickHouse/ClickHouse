-- Tags: distributed

-- A column some tables of `merge` lack is read as the default of the `Merge` type. With a `Distributed` table among them
-- every table computes the query itself, and the default must keep its type and name there.

CREATE TABLE t_a (id UInt64, a UInt8, dec Decimal(10, 2), d128 Decimal(38, 3), m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_b (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_a VALUES (1, 5, 1.5, 3.5, {'k': 1});
INSERT INTO t_b VALUES (10), (11);
CREATE TABLE t_dist_a AS t_a ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_a);
CREATE TABLE t_dist_b AS t_b ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_b);

-- `(a|dist_b)`: the `Distributed` table lacks the columns; `(dist_a|b)`: the `MergeTree` table does.
SELECT 'decimal', min(dec), sum(dec), max(d128), min(dec + 1), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 1;
SELECT 'decimal rollup', dec, count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') GROUP BY dec WITH ROLLUP ORDER BY ALL SETTINGS prefer_localhost_replica = 1;
SELECT 'map', max(m), max(mapKeys(m)), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 1;
SELECT 'map', toTypeName(any(m)), max(m), count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') SETTINGS prefer_localhost_replica = 1;
SELECT 'group by', id, a, count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') GROUP BY id, a ORDER BY ALL SETTINGS prefer_localhost_replica = 1;
SELECT 'order by', id, a FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY a, id SETTINGS prefer_localhost_replica = 1;
SELECT 'limit by', id, a FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id LIMIT 1 BY a SETTINGS prefer_localhost_replica = 1;
SELECT 'uint8', min(a), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 1;

SELECT 'decimal', min(dec), sum(dec), max(d128), min(dec + 1), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 0;
SELECT 'decimal rollup', dec, count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') GROUP BY dec WITH ROLLUP ORDER BY ALL SETTINGS prefer_localhost_replica = 0;
SELECT 'map', max(m), max(mapKeys(m)), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 0;
SELECT 'map', toTypeName(any(m)), max(m), count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') SETTINGS prefer_localhost_replica = 0;
SELECT 'group by', id, a, count() FROM merge(currentDatabase(), '^t_(dist_a|b)$') GROUP BY id, a ORDER BY ALL SETTINGS prefer_localhost_replica = 0;
SELECT 'order by', id, a FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY a, id SETTINGS prefer_localhost_replica = 0;
SELECT 'limit by', id, a FROM merge(currentDatabase(), '^t_(dist_a|b)$') ORDER BY id LIMIT 1 BY a SETTINGS prefer_localhost_replica = 0;
SELECT 'uint8', min(a), count() FROM merge(currentDatabase(), '^t_(a|dist_b)$') SETTINGS prefer_localhost_replica = 0;
