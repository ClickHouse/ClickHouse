-- With `uuid_type_version = 2`, a bare `UUID` must be materialized into every expression that a `CREATE` or
-- an `ALTER` persists, not only into column declarations: leaving `ORDER BY`, `PARTITION BY`, TTL, indices,
-- constraints, projections or mutation expressions out would let the same stored metadata resolve a bare
-- `UUID` to the historical `UUID` later. Type names that a function declares through a string literal
-- (`reinterpret`, `defaultValueOfTypeName`, the `JSONExtract` family including its case-insensitive
-- variants) are materialized as well.

DROP TABLE IF EXISTS t_uuid2_storage;
DROP TABLE IF EXISTS t_uuid2_alter;
DROP TABLE IF EXISTS t_uuid2_mutation;
DROP TABLE IF EXISTS t_uuid2_mv_source;
DROP TABLE IF EXISTS t_uuid2_mv;
DROP TABLE IF EXISTS t_uuid2_reinterpret;
DROP TABLE IF EXISTS t_uuid2_default_value_of_type_name;
DROP TABLE IF EXISTS t_uuid2_json_extract;
DROP TABLE IF EXISTS t_uuid2_json_extract_ci;
DROP TABLE IF EXISTS t_uuid2_json_extract_keys_and_values_ci;
DROP TABLE IF EXISTS t_uuid1_json_extract;
DROP TABLE IF EXISTS t_uuid1_json_extract_ci;

SET uuid_type_version = 2;

SELECT 'storage, index, constraint and projection expressions';
CREATE TABLE t_uuid2_storage
(
    x UUID,
    d Date,
    INDEX idx_x CAST(x, 'UUID') TYPE minmax GRANULARITY 1,
    PROJECTION p_x (SELECT CAST(x, 'UUID') AS y ORDER BY y),
    CONSTRAINT c_x CHECK CAST(x, 'UUID') = x
)
ENGINE = MergeTree
PARTITION BY (toYear(d), CAST(x, 'UUID') = defaultValueOfTypeName('UUID'))
PRIMARY KEY CAST(x, 'UUID')
ORDER BY (CAST(x, 'UUID'), d)
TTL d + toIntervalYear(100) DELETE WHERE CAST(x, 'UUID') != defaultValueOfTypeName('UUID');
SELECT
    partition_key,
    primary_key,
    sorting_key,
    replaceRegexpOne(create_table_query, '^.*(CONSTRAINT .*)\\) ENGINE = .*$', '\\1') AS constraint_and_projection,
    replaceRegexpOne(create_table_query, '^.* (TTL .*) SETTINGS .*$', '\\1') AS ttl_expression
FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_storage' FORMAT Vertical;
SELECT name, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_uuid2_storage';
SELECT name, query FROM system.projections WHERE database = currentDatabase() AND table = 't_uuid2_storage';

SELECT 'ALTER MODIFY ORDER BY, ADD INDEX, ADD PROJECTION, MODIFY TTL';
CREATE TABLE t_uuid2_alter (x UUID, d Date) ENGINE = MergeTree PRIMARY KEY x ORDER BY x;
ALTER TABLE t_uuid2_alter ADD INDEX idx_x CAST(x, 'UUID') TYPE minmax GRANULARITY 1;
ALTER TABLE t_uuid2_alter ADD PROJECTION p_x (SELECT CAST(x, 'UUID') AS y ORDER BY y);
ALTER TABLE t_uuid2_alter ADD COLUMN y UUID, MODIFY ORDER BY (x, CAST(y, 'UUID'));
ALTER TABLE t_uuid2_alter MODIFY TTL d + toIntervalYear(100) DELETE WHERE CAST(x, 'UUID') != defaultValueOfTypeName('UUID');
SELECT
    sorting_key,
    replaceRegexpOne(create_table_query, '^.* (TTL .*) SETTINGS .*$', '\\1') AS ttl_expression
FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_alter' FORMAT Vertical;
SELECT name, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_uuid2_alter';
SELECT name, query FROM system.projections WHERE database = currentDatabase() AND table = 't_uuid2_alter';

SELECT 'mutation expressions';
CREATE TABLE t_uuid2_mutation (k UInt8, x UUID) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_uuid2_mutation VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SET mutations_sync = 2;
ALTER TABLE t_uuid2_mutation UPDATE x = CAST('00000000-0000-0000-0000-000000000001', 'UUID') WHERE k = 1;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_uuid2_mutation';
SELECT x FROM t_uuid2_mutation;

SELECT 'the query of a materialized view, including after MODIFY QUERY';
CREATE TABLE t_uuid2_mv_source (k UInt8, s String) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW t_uuid2_mv ENGINE = MergeTree ORDER BY k AS SELECT k, CAST(s, 'UUID') AS x FROM t_uuid2_mv_source;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_mv' ORDER BY name;
ALTER TABLE t_uuid2_mv MODIFY QUERY SELECT k, accurateCastOrNull(s, 'UUID') AS x FROM t_uuid2_mv_source;
SELECT replaceAll(as_select, currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_uuid2_mv';

SELECT 'a type declared by a function through a string literal';
CREATE TABLE t_uuid2_reinterpret ENGINE = Memory AS SELECT reinterpret(toUInt128(1), 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_reinterpret';
CREATE TABLE t_uuid2_default_value_of_type_name ENGINE = Memory AS SELECT defaultValueOfTypeName('UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_default_value_of_type_name';
CREATE TABLE t_uuid2_json_extract ENGINE = Memory AS SELECT JSONExtract('{"a":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'a', 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_json_extract';
CREATE TABLE t_uuid2_json_extract_ci ENGINE = Memory AS SELECT JSONExtractCaseInsensitive('{"A":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'a', 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_json_extract_ci';
CREATE TABLE t_uuid2_json_extract_keys_and_values_ci ENGINE = Memory AS SELECT JSONExtractKeysAndValuesCaseInsensitive('{"A":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid2_json_extract_keys_and_values_ci';

SET uuid_type_version = 1;

SELECT 'the historical type is unchanged under uuid_type_version = 1';
CREATE TABLE t_uuid1_json_extract ENGINE = Memory AS SELECT JSONExtract('{"a":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'a', 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid1_json_extract';
CREATE TABLE t_uuid1_json_extract_ci ENGINE = Memory AS SELECT JSONExtractCaseInsensitive('{"A":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}', 'a', 'UUID') AS x;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_uuid1_json_extract_ci';

DROP TABLE t_uuid2_storage;
DROP TABLE t_uuid2_alter;
DROP TABLE t_uuid2_mutation;
DROP TABLE t_uuid2_mv;
DROP TABLE t_uuid2_mv_source;
DROP TABLE t_uuid2_reinterpret;
DROP TABLE t_uuid2_default_value_of_type_name;
DROP TABLE t_uuid2_json_extract;
DROP TABLE t_uuid2_json_extract_ci;
DROP TABLE t_uuid2_json_extract_keys_and_values_ci;
DROP TABLE t_uuid1_json_extract;
DROP TABLE t_uuid1_json_extract_ci;
