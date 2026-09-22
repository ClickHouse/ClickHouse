-- A dictionary on the right side of a join that `direct` does not serve (not listed, or a mixed ON
-- condition) is read as an ordinary stream by the hash join.

SET enable_analyzer = 1;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET allow_experimental_join_condition = 1;

DROP DICTIONARY IF EXISTS dict_05223;
DROP TABLE IF EXISTS src_05223;
DROP TABLE IF EXISTS probe_05223;

CREATE TABLE src_05223 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
AS SELECT number, number * 10 FROM numbers(1000);

CREATE DICTIONARY dict_05223 (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_05223' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

CREATE TABLE probe_05223 (id UInt64, p UInt64) ENGINE = MergeTree ORDER BY id
AS SELECT number % 1500, number FROM numbers(3000);

SELECT '-- hash serves the dictionary when direct is not listed';
SELECT count() FROM (EXPLAIN actions = 1 SELECT p.p FROM probe_05223 AS p INNER JOIN dict_05223 AS d ON p.id = d.id SETTINGS join_algorithm = 'hash')
WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT '-- the same rows as direct';
SELECT
    (SELECT (count(), sum(cityHash64(p.p, d.v))) FROM probe_05223 AS p INNER JOIN dict_05223 AS d ON p.id = d.id SETTINGS join_algorithm = 'hash')
    = (SELECT (count(), sum(cityHash64(p.p, d.v))) FROM probe_05223 AS p INNER JOIN dict_05223 AS d ON p.id = d.id SETTINGS join_algorithm = 'direct');
SELECT
    (SELECT (count(), sum(cityHash64(p.p, d.v))) FROM probe_05223 AS p LEFT JOIN dict_05223 AS d ON p.id = d.id SETTINGS join_algorithm = 'hash')
    = (SELECT (count(), sum(cityHash64(p.p, d.v))) FROM probe_05223 AS p LEFT JOIN dict_05223 AS d ON p.id = d.id SETTINGS join_algorithm = 'direct');

SELECT '-- a mixed ON condition, which direct declines, runs on the hash join';
SELECT count() FROM (EXPLAIN actions = 1 SELECT p.p FROM probe_05223 AS p RIGHT JOIN dict_05223 AS d ON p.id = d.id AND p.p < d.v SETTINGS join_algorithm = 'direct,hash')
WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';
SELECT count(), sum(p.p), sum(d.v) FROM probe_05223 AS p INNER JOIN dict_05223 AS d ON p.id = d.id AND p.p < d.v SETTINGS join_algorithm = 'direct,hash';
SELECT count(), sum(p.p), sum(d.v) FROM probe_05223 AS p RIGHT JOIN dict_05223 AS d ON p.id = d.id AND p.p < d.v SETTINGS join_algorithm = 'direct,hash';
SELECT count(), sum(p.p), sum(d.v) FROM probe_05223 AS p FULL JOIN dict_05223 AS d ON p.id = d.id AND p.p < d.v SETTINGS join_algorithm = 'direct,hash';

DROP DICTIONARY dict_05223;
DROP TABLE src_05223;
DROP TABLE probe_05223;
