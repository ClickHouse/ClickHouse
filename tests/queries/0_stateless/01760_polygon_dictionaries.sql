-- Tags: no-parallel

DROP DATABASE IF EXISTS 01760_db;
CREATE DATABASE 01760_db;

DROP TABLE IF EXISTS 01760_db.polygons;
CREATE TABLE 01760_db.polygons (key Array(Array(Array(Tuple(Float64, Float64)))), name String, value UInt64, value_nullable Nullable(UInt64)) ENGINE = Memory;
INSERT INTO 01760_db.polygons VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421, 421);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422, NULL);
INSERT INTO 01760_db.polygons VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423, 423);
INSERT INTO 01760_db.polygons VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424, NULL);

DROP TABLE IF EXISTS 01760_db.points;
CREATE TABLE 01760_db.points (x Float64, y Float64, def_i UInt64, def_s String) ENGINE = Memory;
INSERT INTO 01760_db.points VALUES (0.1, 0.0, 112, 'aax');
INSERT INTO 01760_db.points VALUES (-0.1, 0.0, 113, 'aay');
INSERT INTO 01760_db.points VALUES (0.0, 1.1, 114, 'aaz');
INSERT INTO 01760_db.points VALUES (0.0, -1.1, 115, 'aat');
INSERT INTO 01760_db.points VALUES (3.0, 3.0, 22, 'bb');

DROP DICTIONARY IF EXISTS 01760_db.dict_array;
CREATE DICTIONARY 01760_db.dict_array
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 10,
    value_nullable Nullable(UInt64) DEFAULT 20
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons' DB '01760_db'))
LIFETIME(0)
LAYOUT(POLYGON());

SELECT 'dictGet';

SELECT tuple(x, y) as key,
    dictGet('01760_db.dict_array', 'name', key),
    dictGet('01760_db.dict_array', 'value', key),
    dictGet('01760_db.dict_array', 'value_nullable', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dictGetOrDefault';

SELECT tuple(x, y) as key,
    dictGetOrDefault('01760_db.dict_array', 'name', key, 'DefaultName'),
    dictGetOrDefault('01760_db.dict_array', 'value', key, 30),
    dictGetOrDefault('01760_db.dict_array', 'value_nullable', key, 40)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'dictHas';

SELECT tuple(x, y) as key,
    dictHas('01760_db.dict_array', key),
    dictHas('01760_db.dict_array', key),
    dictHas('01760_db.dict_array', key)
FROM 01760_db.points
ORDER BY x, y;

SELECT 'check NaN or infinite point input';
SELECT tuple(nan, inf) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(nan, nan) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(inf, nan) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}
SELECT tuple(inf, inf) as key, dictGet('01760_db.dict_array', 'name', key); --{serverError 36}

DROP DICTIONARY 01760_db.dict_array;
DROP TABLE 01760_db.points;
DROP TABLE 01760_db.polygons;
DROP DATABASE 01760_db;
