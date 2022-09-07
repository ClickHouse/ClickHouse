-- This test checks, that common SQL operations work
-- with mixed columns (sparse and full) in table.

DROP TABLE IF EXISTS t_sparse_full;

CREATE TABLE t_sparse_full (id UInt64, u UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 32,
ratio_of_defaults_for_sparse_serialization = 0.1;

SYSTEM STOP MERGES t_sparse_full;

INSERT INTO t_sparse_full
SELECT
    number,
    if (number % 10 = 0, number, 0),
    if (number % 7 = 0, toString(number), '')
FROM numbers(1000);

INSERT INTO t_sparse_full
SELECT
    number,
    number,
    toString(number)
FROM numbers(500);

SELECT name, column, serialization_kind
FROM system.parts_columns WHERE table = 't_sparse_full' AND database = currentDatabase() AND active
ORDER BY name, column;

SELECT id, u FROM t_sparse_full ORDER BY id, u LIMIT 4;
SELECT '======';
SELECT id, u FROM t_sparse_full ORDER BY id, u LIMIT 4 SETTINGS optimize_read_in_order = 0;
SELECT '======';
SELECT id, u, s FROM t_sparse_full ORDER BY u DESC LIMIT 3;
SELECT '======';
SELECT id, u, s FROM t_sparse_full WHERE u != 0 ORDER BY u DESC LIMIT 3;
SELECT '======';
SELECT id % 3 AS k, sum(u) FROM t_sparse_full WHERE u != 0 GROUP BY k ORDER BY k;
SELECT '======';
SELECT uniqExact(u) FROM t_sparse_full WHERE s != '';
SELECT '======';
SELECT toUInt32(s) % 5 AS k, groupUniqArray(u % 4) FROM t_sparse_full WHERE s != '' GROUP BY k ORDER BY k;
SELECT max(range(id % 10)[u]) FROM t_sparse_full;
SELECT '======';
SELECT id, u, s FROM remote('127.0.0.{1,2}', currentDatabase(), t_sparse_full) ORDER BY id, u, s LIMIT 5;
SELECT '======';
SELECT sum(u) FROM t_sparse_full GROUP BY id % 3 AS k WITH TOTALS ORDER BY k;
SELECT '======';
SELECT sum(u) AS value FROM t_sparse_full GROUP BY id % 3 AS k WITH ROLLUP ORDER BY value;
SELECT '======';
SELECT sum(u) AS value FROM t_sparse_full GROUP BY id % 3 AS k WITH CUBE ORDER BY value;
SELECT '======';
SELECT sum(id) FROM t_sparse_full GROUP BY u % 3 AS k ORDER BY k;
SELECT '======';
SELECT count() FROM t_sparse_full WHERE u % 4 = 0;
SELECT '======';
SELECT count() FROM t_sparse_full WHERE u IN (SELECT u FROM t_sparse_full WHERE id % 4 = 2);
SELECT '======';
SELECT DISTINCT u FROM t_sparse_full ORDER BY id LIMIT 5;

SELECT '======';

SELECT id, u, s FROM t_sparse_full INNER JOIN
(
    SELECT number * 3 AS u FROM numbers(10)
) AS t1 USING(u) ORDER BY id, u, s LIMIT 5;

SELECT '======';

SELECT id, u, s FROM t_sparse_full FULL JOIN
(
    SELECT number * 3 AS u FROM numbers(10)
) AS t1 USING(u) ORDER BY id, u, s LIMIT 5;

SELECT '======';

SELECT id, u, s FROM (SELECT number * 2 AS u FROM numbers(10)) AS t1
INNER JOIN t_sparse_full USING(u) ORDER BY id, u, s LIMIT 5;

SELECT '======';

SELECT id, u, s FROM (SELECT number * 2 AS u FROM numbers(10)) AS t1
FULL JOIN t_sparse_full USING(u) ORDER BY id, u, s LIMIT 5;

SELECT '======';

SELECT id, u, s FROM (SELECT u FROM t_sparse_full) AS t1
FULL JOIN t_sparse_full USING(u) ORDER BY id, u, s LIMIT 5;

SYSTEM START MERGES t_sparse_full;

OPTIMIZE TABLE t_sparse_full FINAL;

SELECT '======';

SELECT column, serialization_kind
FROM system.parts_columns WHERE table = 't_sparse_full' AND database = currentDatabase() AND active
ORDER BY name, column;

SELECT '======';

SELECT id, u, s FROM t_sparse_full ORDER BY u DESC LIMIT 3;

DROP TABLE t_sparse_full;
