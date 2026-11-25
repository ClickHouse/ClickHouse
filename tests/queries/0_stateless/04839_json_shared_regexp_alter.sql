-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS alter_04839;

CREATE TABLE alter_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=2, SHARED REGEXP '^force$')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;

SYSTEM STOP MERGES alter_04839;

INSERT INTO alter_04839 SELECT number, toJSONString(map('force', number, 'keep', number)) FROM numbers(4);
INSERT INTO alter_04839 SELECT number + 4, toJSONString(map('force', number + 4)) FROM numbers(4);

SELECT
    'initial',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- Removing the rule is metadata-only: old parts keep their type and placement until they are merged.
ALTER TABLE alter_04839 MODIFY COLUMN j JSON(max_dynamic_paths=2);

SELECT 'mutations', count() FROM system.mutations WHERE database=currentDatabase() AND table='alter_04839';

SELECT
    'removed',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;
SYSTEM STOP MERGES alter_04839;

SELECT
    'reattached types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'reattached',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- The next merge uses the current type, so the frequent path formerly kept in shared data is promoted.
SYSTEM START MERGES alter_04839;
OPTIMIZE TABLE alter_04839 FINAL;

SELECT
    'merged without rule types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'merged without rule',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- Adding the rule back is metadata-only too, and the next merge moves the path back to shared data.
ALTER TABLE alter_04839 MODIFY COLUMN j JSON(max_dynamic_paths=2, SHARED REGEXP '^force$');

SELECT 'mutations', count() FROM system.mutations WHERE database=currentDatabase() AND table='alter_04839';

SELECT
    'added',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

OPTIMIZE TABLE alter_04839 FINAL;

SELECT
    'merged with rule types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'merged with rule',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

DROP TABLE alter_04839;

-- A CAST between types that differ only in SHARED REGEXP keeps value types and applies the new rules.
CREATE TABLE cast_04839 (t Tuple(j JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO cast_04839 SETTINGS input_format_try_infer_dates = 0, input_format_try_infer_datetimes = 0 VALUES (tuple('{"d":"2020-01-01","k":"2020-01-02"}'));
SELECT 'cast', dynamicType(c.d), dynamicType(c.k), JSONDynamicPaths(c), JSONSharedDataPaths(c) FROM (SELECT CAST(t.j, 'JSON(SHARED REGEXP \'^d$\')') AS c FROM cast_04839);
ALTER TABLE cast_04839 MODIFY COLUMN t Tuple(j JSON(SHARED REGEXP '^d$')) SETTINGS mutations_sync = 2;
SELECT 'tuple alter', dynamicType(t.j.d), dynamicType(t.j.k), JSONDynamicPaths(t.j), JSONSharedDataPaths(t.j) FROM cast_04839;
DROP TABLE cast_04839;

-- A rules-only ALTER keeps skip indexes of old parts; a timezone change in the same ALTER does not.
CREATE TABLE index_04839 (id UInt64, j JSON(a DateTime('UTC')), INDEX paths JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1, INDEX text toString(j) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO index_04839 SELECT number, if(number = 7, '{"needle":1}', toJSONString(map('a', toString(toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600)))) FROM numbers(24);
ALTER TABLE index_04839 MODIFY COLUMN j JSON(a DateTime('UTC'), SHARED REGEXP '^zzz$');
SELECT 'rules-only alter', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM index_04839 WHERE has(JSONAllPaths(j), 'needle') SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Granules%';
ALTER TABLE index_04839 MODIFY COLUMN j JSON(a DateTime('Asia/Tokyo'), SHARED REGEXP '^yyy$');
SELECT 'rules and timezone alter', id FROM index_04839 WHERE toString(j) = '{"a":"2020-01-01 09:00:00"}';
DROP TABLE index_04839;

-- A skip index that reads the column inside a folded constant would keep granules built from the old type name.
CREATE TABLE index_04839 (id UInt64, j JSON, INDEX ix concat(toTypeName(j), toString(id)) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO index_04839 SELECT number, '{"a":1}' FROM numbers(24);
ALTER TABLE index_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'folded index', count() FROM index_04839 WHERE id = 7 SETTINGS use_skip_indexes = 1;
SELECT 'folded index', count() FROM index_04839 WHERE id = 7 SETTINGS use_skip_indexes = 0;
DROP TABLE index_04839;
-- The same when the index also reads the column directly; dropping the index in the same ALTER is allowed.
CREATE TABLE index_04839 (id UInt64, j JSON, INDEX ix concat(toTypeName(j), toString(j)) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO index_04839 SELECT number, toJSONString(map('a', number)) FROM numbers(24);
ALTER TABLE index_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'folded and direct index', count() FROM index_04839 WHERE toString(j) = '{"a":7}' SETTINGS use_skip_indexes = 1;
SELECT 'folded and direct index', count() FROM index_04839 WHERE toString(j) = '{"a":7}' SETTINGS use_skip_indexes = 0;
ALTER TABLE index_04839 DROP INDEX ix, MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$');
DROP TABLE index_04839;

-- A rules-only ALTER rewrites no data, so it is refused when a key or a projection is computed from the whole column.
CREATE TABLE key_04839 (id UInt64, j JSON) ENGINE = MergeTree ORDER BY (dynamicType(CAST(j, 'Dynamic')), id);
ALTER TABLE key_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE key_04839;
CREATE TABLE key_04839 (id UInt64, j JSON) ENGINE = MergeTree PARTITION BY dynamicType(CAST(j, 'Dynamic')) ORDER BY id;
ALTER TABLE key_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE key_04839;
CREATE TABLE key_04839 (id UInt64, j JSON, PROJECTION p (SELECT dynamicType(CAST(j, 'Dynamic')) AS ty, count() GROUP BY ty)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE key_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE key_04839;
-- A projection that reads the column through an ALIAS column or a chain of them.
CREATE TABLE key_04839 (id UInt64, j JSON, a ALIAS j, PROJECTION p (SELECT dynamicType(CAST(a, 'Dynamic')) AS ty, count() GROUP BY ty)) ENGINE = MergeTree ORDER BY id;
INSERT INTO key_04839 SELECT number, '{"x":1}' FROM numbers(4);
ALTER TABLE key_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE key_04839;
CREATE TABLE key_04839 (id UInt64, j JSON, a ALIAS j, b ALIAS a, PROJECTION p (SELECT id, any(dynamicType(CAST(b, 'Dynamic'))) GROUP BY id)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE key_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE key_04839;

-- A stored MATERIALIZED column computed from the whole column, directly or through an ALIAS column, would keep its old values.
CREATE TABLE mat_04839 (id UInt64, j JSON, ty String MATERIALIZED toTypeName(j)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mat_04839 SELECT number, '{"x":1}' FROM numbers(4);
ALTER TABLE mat_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'materialized', ty, count() FROM mat_04839 GROUP BY ty;
DROP TABLE mat_04839;
CREATE TABLE mat_04839 (id UInt64, j JSON, a ALIAS j, ty String MATERIALIZED dynamicType(CAST(a, 'Dynamic'))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE mat_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE mat_04839;
-- A MATERIALIZED column of a subcolumn does not depend on the rules.
CREATE TABLE mat_04839 (id UInt64, j JSON(a UInt32), x UInt32 MATERIALIZED j.a) ENGINE = MergeTree ORDER BY id;
INSERT INTO mat_04839 SELECT number, toJSONString(map('a', number)) FROM numbers(4);
ALTER TABLE mat_04839 MODIFY COLUMN j JSON(a UInt32, SHARED REGEXP '^x$');
SELECT 'materialized subcolumn', sum(x) FROM mat_04839;
DROP TABLE mat_04839;

-- A key on a subcolumn and a projection that stores the column as is do not depend on the rules.
CREATE TABLE key_04839 (id UInt64, j JSON(a UInt32), PROJECTION p (SELECT * ORDER BY j.a)) ENGINE = MergeTree ORDER BY (j.a, id) SETTINGS index_granularity = 1;
INSERT INTO key_04839 SELECT number, toJSONString(map('a', number)) FROM numbers(24);
ALTER TABLE key_04839 MODIFY COLUMN j JSON(a UInt32, SHARED REGEXP '^zzz$');
SELECT 'subcolumn key', count() FROM key_04839 WHERE j.a = 5;
DROP TABLE key_04839;

-- Old parts keep their TTL infos, so a TTL that reads the whole column refuses a rules-only ALTER too.
CREATE TABLE ttl_04839 (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id TTL if(dynamicType(CAST(j, 'Dynamic')) = 'JSON', toDateTime('2000-01-01'), toDateTime('2100-01-01'));
ALTER TABLE ttl_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE ttl_04839;
CREATE TABLE ttl_04839 (id UInt64, j JSON, x UInt64 TTL if(dynamicType(CAST(j, 'Dynamic')) = 'JSON', toDateTime('2000-01-01'), toDateTime('2100-01-01'))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE ttl_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE ttl_04839;
-- A TTL whose read of the column is folded into a constant.
CREATE TABLE ttl_04839 (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id TTL if(toTypeName(j) = 'JSON', toDateTime('2000-01-01'), toDateTime('2100-01-01')) + toIntervalSecond(id);
ALTER TABLE ttl_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^zzz$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE ttl_04839;
CREATE TABLE ttl_04839 (id UInt64, j JSON(a UInt32)) ENGINE = MergeTree ORDER BY id TTL toDateTime('2100-01-01') + toIntervalSecond(j.a);
INSERT INTO ttl_04839 SELECT number, toJSONString(map('a', number)) FROM numbers(24);
ALTER TABLE ttl_04839 MODIFY COLUMN j JSON(a UInt32, SHARED REGEXP '^zzz$');
OPTIMIZE TABLE ttl_04839 FINAL;
SELECT 'subcolumn ttl', count() FROM ttl_04839;
DROP TABLE ttl_04839;

-- A lambda parameter named like the column is not a read of it.
CREATE TABLE lambda_04839 (id UInt64, j JSON, xs Array(UInt64), m Array(UInt64) MATERIALIZED arrayMap(j -> j + 1, xs)) ENGINE = MergeTree ORDER BY id;
INSERT INTO lambda_04839 (id, j, xs) VALUES (1, '{"x":1}', [1, 2]);
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$');
SELECT 'lambda materialized', toTypeName(j), m FROM lambda_04839;
DROP TABLE lambda_04839;
CREATE TABLE lambda_04839 (id UInt64, j JSON, xs Array(UInt64), INDEX ix concat(toTypeName(arrayMap(j -> j + 1, xs)), toString(id)) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO lambda_04839 VALUES (1, '{"x":1}', [1, 2]);
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$');
SELECT 'lambda index', toTypeName(j), id FROM lambda_04839 WHERE concat(toTypeName(arrayMap(j -> j + 1, xs)), toString(id)) = 'Array(UInt64)1';
DROP TABLE lambda_04839;
CREATE TABLE lambda_04839 (id UInt64, d Date, j JSON, xs Array(UInt64)) ENGINE = MergeTree ORDER BY id TTL d + toIntervalDay(arraySum(arrayMap(j -> j, xs)));
INSERT INTO lambda_04839 VALUES (1, '2100-01-01', '{"x":1}', [1, 2]);
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$');
SELECT 'lambda ttl', toTypeName(j), id FROM lambda_04839;
DROP TABLE lambda_04839;
-- A lambda body that reads the column still reads it.
CREATE TABLE lambda_04839 (id UInt64, j JSON, xs Array(UInt64), m Array(UInt64) MATERIALIZED arrayMap(x -> x + length(toString(j)), xs)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE lambda_04839;
CREATE TABLE lambda_04839 (id UInt64, j JSON, xs Array(UInt64), INDEX ix concat(toTypeName(arrayMap(x -> j, xs)), toString(id)) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE lambda_04839;
CREATE TABLE lambda_04839 (id UInt64, d Date, j JSON, xs Array(UInt64)) ENGINE = MergeTree ORDER BY id TTL d + toIntervalDay(arraySum(arrayMap(x -> length(toString(j)), xs)));
ALTER TABLE lambda_04839 MODIFY COLUMN j JSON(SHARED REGEXP '^x$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE lambda_04839;
