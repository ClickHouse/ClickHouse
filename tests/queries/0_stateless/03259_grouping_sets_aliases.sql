DROP TABLE IF EXISTS users;
CREATE TABLE users (name String, score UInt8, user_level String ALIAS multiIf(score <= 3, 'LOW', score <= 6, 'MEDIUM', 'HIGH') ) ENGINE=MergeTree ORDER BY name;

INSERT INTO users VALUES ('a',1),('b',2),('c', 50);

SELECT user_level as level_alias, uniq(name) as name_alias, grouping(level_alias) as _totals
FROM remote('127.0.0.{1,2}', currentDatabase(), users)
GROUP BY GROUPING SETS ((level_alias))
ORDER BY name_alias DESC;
