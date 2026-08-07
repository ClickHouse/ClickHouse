-- Tags: distributed

DROP TABLE IF EXISTS test_distributed;
DROP TABLE IF EXISTS test_local;

SET prefer_localhost_replica = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/36279
CREATE TABLE test_local (text String, text2 String) ENGINE = MergeTree() ORDER BY text;
CREATE TABLE test_distributed (text String, text2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local);
INSERT INTO test_distributed SELECT randomString(100) AS text, randomString(100) AS text2 FROM system.numbers LIMIT 1;

SET joined_subquery_requires_alias = 0;

SELECT COUNT() AS count
FROM test_distributed
INNER JOIN
(
    SELECT text
    FROM test_distributed
    WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
) USING (text)
WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
;

DROP TABLE IF EXISTS test_distributed;
DROP TABLE IF EXISTS test_local;

DROP TABLE IF EXISTS user_local;
DROP TABLE IF EXISTS user_all;
DROP TABLE IF EXISTS event;

-- https://github.com/ClickHouse/ClickHouse/issues/36300
CREATE TABLE user_local ( id Int64, name String, age Int32 )
ENGINE = MergeTree ORDER BY name;

CREATE TABLE user_all ( id Int64, name String, age Int32 )
ENGINE = Distributed('test_shard_localhost', currentDatabase(), user_local, rand());

CREATE TABLE event ( id Int64, user_id Int64, content String, created_time DateTime )
ENGINE = MergeTree ORDER BY user_id;

INSERT INTO user_local (id, name, age) VALUES (1, 'aaa', 21);
INSERT INTO event (id, user_id, content, created_time) VALUES(1, 1, 'hello', '2022-01-05 12:00:00');

SELECT
  u.name user_name,
  20 AS age_group
FROM user_all u
LEFT JOIN event e ON u.id = e.user_id
WHERE (u.age >= 20 AND u.age < 30)
AND e.created_time > '2022-01-01';

DROP TABLE IF EXISTS user_local;
DROP TABLE IF EXISTS user_all;
DROP TABLE IF EXISTS event;
