-- The stored JSON text is a part of the value: values with the same paths and values but differently formatted
-- JSON text are different values, so they are not equal and not grouped together.

DROP TABLE IF EXISTS t_json_source_semantics;
CREATE TABLE t_json_source_semantics (id UInt64, json JSON(with_source=1)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_source_semantics VALUES (1, '{"a" : 42}'), (2, '{"a":42}'), (3, '{  "a"  :  42  }');

SELECT 'equal objects with different text';
SELECT count(), countDistinct(json) FROM t_json_source_semantics;
SELECT json FROM t_json_source_semantics GROUP BY json;
SELECT DISTINCT json FROM t_json_source_semantics;
SELECT uniqExact(json), uniqExact(cityHash64(json)) FROM t_json_source_semantics;

SELECT 'comparison with a literal';
SELECT count() FROM t_json_source_semantics WHERE json = '{"a" : 42}'::JSON(with_source=1);
SELECT id, json = (SELECT json FROM t_json_source_semantics WHERE id = 1) FROM t_json_source_semantics ORDER BY id;

SELECT 'source is kept by aggregation';
SELECT json.__source FROM (SELECT json FROM t_json_source_semantics GROUP BY json) ORDER BY json.__source;
SELECT json.__source FROM (SELECT any(json) AS json FROM t_json_source_semantics);

SELECT 'source is kept when values are just copied';
SELECT id, json.__source FROM t_json_source_semantics ORDER BY id;
SELECT id, json.__source FROM (SELECT * FROM t_json_source_semantics WHERE id != 2) ORDER BY id;
SELECT j1.id, j1.json.__source FROM t_json_source_semantics AS j1 JOIN t_json_source_semantics AS j2 ON j1.id = j2.id ORDER BY j1.id;

SELECT 'typed paths keep their types in the created text';
DROP TABLE IF EXISTS t_json_source_typed;
CREATE TABLE t_json_source_typed (json JSON(with_source=1, d Date, n Decimal(10, 2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_source_typed VALUES ('{"d" : "2020-01-01", "n" : 42.42}');
SELECT json.__source FROM t_json_source_typed;
-- A value without the source gets the text created from its object.
SELECT CAST(CAST(json AS JSON(d Date, n Decimal(10, 2))) AS JSON(with_source=1, d Date, n Decimal(10, 2))).__source FROM t_json_source_typed;

DROP TABLE t_json_source_typed;
DROP TABLE t_json_source_semantics;
