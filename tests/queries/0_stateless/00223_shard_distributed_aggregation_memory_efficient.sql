-- Tags: distributed

SET max_block_size = 1000;

DROP TABLE IF EXISTS numbers_10_00223;
CREATE TABLE numbers_10_00223 ENGINE = Log AS SELECT * FROM system.numbers LIMIT 10000;

-- `shardNum()` is evaluated on each shard, so the two shards of one query can read different sets of keys.

SET distributed_aggregation_memory_efficient = 0;
SET group_by_two_level_threshold = 1000;

SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY number);

SET distributed_aggregation_memory_efficient = 0;
SET group_by_two_level_threshold = 7;

SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1000;

SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 7;

SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1;

SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY number);
SELECT sum(c = 1), sum(c = 2) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1000;

SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number >= 5 AND number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) AND number >= (shardNum() = 1 ? 0 : 5) GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) AND number >= (shardNum() = 1 ? 5 : 0) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1;

SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number >= 5 AND number < 10 GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) AND number >= (shardNum() = 1 ? 0 : 5) GROUP BY number);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) AND number >= (shardNum() = 1 ? 5 : 0) GROUP BY number);

SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 7;

SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number AS k1, number + 1 AS k2, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 5 GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number AS k1, number + 1 AS k2, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 10 GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number AS k1, number + 1 AS k2, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 5 : 10) GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 1), sum(c = 2), sum(c) FROM (SELECT number AS k1, number + 1 AS k2, count() AS c FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 10 : 5) GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);

SELECT sum(c = 20), sum(c = 10), sum(u != 10) FROM (SELECT intDiv(number, 10) AS k1, k1 + 1 AS k2, count() AS c, uniq(number) AS u FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 50 GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 20), sum(c = 10), sum(u != 10) FROM (SELECT intDiv(number, 10) AS k1, k1 + 1 AS k2, count() AS c, uniq(number) AS u FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < 100 GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 20), sum(c = 10), sum(u != 10) FROM (SELECT intDiv(number, 10) AS k1, k1 + 1 AS k2, count() AS c, uniq(number) AS u FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 50 : 100) GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);
SELECT sum(c = 20), sum(c = 10), sum(u != 10) FROM (SELECT intDiv(number, 10) AS k1, k1 + 1 AS k2, count() AS c, uniq(number) AS u FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223) WHERE number < (shardNum() = 1 ? 100 : 50) GROUP BY k1, k2 HAVING count() > 0 ORDER BY k1, k2);

DROP TABLE numbers_10_00223;

SELECT count() FROM remote('127.0.0.{2,3}', system.one);
