DROP TABLE IF EXISTS numbers_10_00290;
SET max_block_size = 1000;
CREATE TABLE numbers_10_00290 ENGINE = Log AS SELECT * FROM system.numbers LIMIT 10000;
SET distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 5000;

SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;
SELECT concat(toString(number), arrayStringConcat(arrayMap(x -> '.', range(number % 10)))) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00290) WHERE number < (randConstant() % 2 ? 4999 : 10000) GROUP BY k ORDER BY k LIMIT 10;

DROP TABLE numbers_10_00290;
