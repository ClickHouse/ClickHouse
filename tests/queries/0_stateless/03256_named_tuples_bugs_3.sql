-- https://github.com/ClickHouse/ClickHouse/issues/69732

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users2;

CREATE TABLE users (id UInt8, city String, name String) ENGINE=Memory;
CREATE TABLE users2 (
  id UInt8,
  city_name_uniq AggregateFunction(uniq, Tuple(String,String))
)
ENGINE=AggregatingMergeTree()
ORDER BY (id);

CREATE MATERIALIZED VIEW test_mv TO users2 AS
SELECT
  id,
  uniqState((city, name)) AS city_name_uniq
FROM users
GROUP BY id;

INSERT INTO users VALUES (1, 'London', 'John');
INSERT INTO users VALUES (1, 'Berlin', 'Ksenia');
INSERT INTO users VALUES (2, 'Paris', 'Alice');

SELECT '---- users:';
SELECT * FROM users;

SELECT '---- users2:';
SELECT id, uniqMerge(city_name_uniq) FROM users2
GROUP BY id;

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users2;
