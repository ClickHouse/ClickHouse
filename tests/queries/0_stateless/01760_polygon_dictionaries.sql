-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.polygons;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.polygons (key Array(Array(Array(Tuple(Float64, Float64)))), name String, value UInt64, value_nullable Nullable(UInt64)) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.polygons VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Click East', 421, 421);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.polygons VALUES ([[[(-1, 1), (1, 1), (1, 3), (-1, 3)]]], 'Click North', 422, NULL);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.polygons VALUES ([[[(-3, 1), (-3, -1), (0, -1), (0, 1)]]], 'Click South', 423, 423);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.polygons VALUES ([[[(-1, -1), (1, -1), (1, -3), (-1, -3)]]], 'Click West', 424, NULL);

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.points;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.points (x Float64, y Float64, def_i UInt64, def_s String) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.points VALUES (0.1, 0.0, 112, 'aax');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.points VALUES (-0.1, 0.0, 113, 'aay');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.points VALUES (0.0, 1.1, 114, 'aaz');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.points VALUES (0.0, -1.1, 115, 'aat');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.points VALUES (3.0, 3.0, 22, 'bb');

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.dict_array;
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict_array
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String DEFAULT 'qqq',
    value UInt64 DEFAULT 10,
    value_nullable Nullable(UInt64) DEFAULT 20
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons' DB currentDatabase()))
LIFETIME(0)
LAYOUT(POLYGON())
SETTINGS(dictionary_use_async_executor=1, max_threads=8)
;

SELECT 'dictGet';

SELECT tuple(x, y) as key,
    dictGet('dict_array', 'name', key),
    dictGet('dict_array', 'value', key),
    dictGet('dict_array', 'value_nullable', key)
FROM {CLICKHOUSE_DATABASE_1:Identifier}.points
ORDER BY x, y;

SELECT 'dictGetOrDefault';

SELECT tuple(x, y) as key,
    dictGetOrDefault('dict_array', 'name', key, 'DefaultName'),
    dictGetOrDefault('dict_array', 'value', key, 30),
    dictGetOrDefault('dict_array', 'value_nullable', key, 40)
FROM {CLICKHOUSE_DATABASE_1:Identifier}.points
ORDER BY x, y;

SELECT 'dictHas';

SELECT tuple(x, y) as key,
    dictHas('dict_array', key),
    dictHas('dict_array', key),
    dictHas('dict_array', key)
FROM {CLICKHOUSE_DATABASE_1:Identifier}.points
ORDER BY x, y;

SELECT 'check NaN or infinite point input';
SELECT tuple(nan, inf) as key, dictGet('dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT tuple(nan, nan) as key, dictGet('dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT tuple(inf, nan) as key, dictGet('dict_array', 'name', key); --{serverError BAD_ARGUMENTS}
SELECT tuple(inf, inf) as key, dictGet('dict_array', 'name', key); --{serverError BAD_ARGUMENTS}

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict_array;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.points;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.polygons;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
