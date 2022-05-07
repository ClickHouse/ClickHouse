DROP TABLE IF EXISTS one;
CREATE TABLE one(dummy UInt8) ENGINE = Memory;
SET max_merged_block_size=102400;

SELECT database, t.name
    FROM system.tables AS t
    ALL INNER JOIN (SELECT name AS database FROM system.databases) AS db USING database
    WHERE database = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT database, t.name
    FROM (SELECT name AS database FROM system.databases) AS db
    ALL INNER JOIN system.tables AS t USING database
    WHERE database = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT database, t.name
    FROM (SELECT name, database FROM system.tables) AS t
    ALL INNER JOIN (SELECT name AS database FROM system.databases) AS db USING database
    WHERE database = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT x, t.name
    FROM (SELECT name, database AS x FROM system.tables) AS t
    ALL INNER JOIN (SELECT name AS x FROM system.databases) AS db USING x
    WHERE x = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT database, t.name
    FROM (SELECT name, database FROM system.tables) AS t
    JOIN (SELECT name AS database FROM system.databases) AS db USING database
    WHERE database = 'system' AND t.name = 'one'
    SETTINGS join_default_strictness = 'ALL'
    FORMAT PrettyCompactNoEscapes;

SELECT x, t.name
    FROM (SELECT name, database AS x FROM system.tables) AS t
    JOIN (SELECT name AS x FROM system.databases) AS db USING x
    WHERE x = 'system' AND t.name = 'one'
    SETTINGS join_default_strictness = 'ALL'
    FORMAT PrettyCompactNoEscapes;

SET join_default_strictness = 'ALL';

SELECT database, t.name
    FROM (SELECT * FROM system.tables) AS t
    JOIN (SELECT name, name AS database FROM system.databases) AS db USING database
    WHERE db.name = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT db.x, t.name
    FROM (SELECT name, database AS x FROM system.tables) AS t
    JOIN (SELECT name AS x FROM system.databases) AS db USING x
    WHERE x = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT db.name, t.name
    FROM (SELECT name, database FROM system.tables WHERE name = 'one') AS t
    JOIN (SELECT name FROM system.databases WHERE name = 'system') AS db ON t.database = db.name
    FORMAT PrettyCompactNoEscapes;

SELECT db.name, t.name
    FROM system.tables AS t
    JOIN (SELECT * FROM system.databases WHERE name = 'system') AS db ON t.database = db.name
    WHERE t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT t.database, t.name
    FROM system.tables AS t
    JOIN (SELECT name, name AS database FROM system.databases) AS db ON t.database = db.name
    WHERE t.database = 'system' AND t.name = 'one'
    FORMAT PrettyCompactNoEscapes;

SELECT t.database, t.name
    FROM system.tables t
    ANY LEFT JOIN (SELECT 'system' AS base, 'one' AS name) db USING name
    WHERE t.database = db.base
    FORMAT PrettyCompactNoEscapes;

SELECT count(t.database)
    FROM (SELECT * FROM system.tables WHERE name = 'one') AS t
    JOIN system.databases AS db ON t.database = db.name;

SELECT count(db.name)
    FROM system.tables AS t
    JOIN system.databases AS db ON t.database = db.name
    WHERE t.name = 'one';

SELECT count()
    FROM system.tables AS t
    JOIN system.databases AS db ON db.name = t.database
    WHERE t.name = 'one';

DROP TABLE one;

DROP TABLE IF EXISTS join_opt_t1;
DROP TABLE IF EXISTS join_opt_t2;
DROP TABLE IF EXISTS join_opt_t3;

CREATE TABLE join_opt_t1(a int, b int)engine=MergeTree ORDER BY a;
CREATE TABLE join_opt_t2(a int, b int)engine=MergeTree ORDER BY a;
CREATE TABLE join_opt_t3(a int, b int)engine=MergeTree ORDER BY a;
INSERT INTO join_opt_t1 SELECT number, number+1 from numbers(1000);
INSERT INTO join_opt_t2 SELECT number, number+1 from numbers(200);
INSERT INTO join_opt_t3 SELECT number, number+1 from numbers(100);

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a
    INNER JOIN join_opt_t3
    ON join_opt_t2.b = join_opt_t3.b;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a AND join_opt_t1.a < 3;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a AND join_opt_t1.b = join_opt_t2.b;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a OR join_opt_t1.b = join_opt_t2.b;

DROP TABLE join_opt_t1;
DROP TABLE join_opt_t2;
DROP TABLE join_opt_t3;
