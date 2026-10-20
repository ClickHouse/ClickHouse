-- Tags: no-parallel-replicas
-- ArrayJoin Bloom-filter analysis must preserve the effective arrayJoin values.

DROP TABLE IF EXISTS t_bf_aj_wrappers;
DROP TABLE IF EXISTS t_bf_aj_target;
DROP TABLE IF EXISTS t_bf_aj_default;
DROP TABLE IF EXISTS t_bf_aj_nullable;
DROP TABLE IF EXISTS t_bf_aj_uint8;
DROP TABLE IF EXISTS t_bf_aj_numeric;
DROP TABLE IF EXISTS t_bf_aj_expression;
DROP TABLE IF EXISTS t_bf_aj_collision;
DROP TABLE IF EXISTS t_bf_aj_deeper_collision;
DROP TABLE IF EXISTS t_bf_aj_concat_expression;
DROP TABLE IF EXISTS t_bf_aj_physical_types;
DROP TABLE IF EXISTS t_bf_aj_invalid;

CREATE TABLE t_bf_aj_wrappers
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_aj_wrappers VALUES
    (0, []),
    (1, ['target']),
    (2, ['target', 'target']),
    (3, ['']),
    (4, ['01']),
    (5, ['1']),
    (6, ['other']);

CREATE TABLE t_bf_aj_target (value String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_aj_target VALUES ('target');

CREATE TABLE t_bf_aj_default (value String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_aj_default VALUES ('target'), ('');

CREATE TABLE t_bf_aj_nullable (value Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_aj_nullable VALUES ('target'), (NULL);

CREATE TABLE t_bf_aj_uint8 (value UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_aj_uint8 VALUES (1);

CREATE TABLE t_bf_aj_numeric
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_numeric VALUES (4, ['01']), (5, ['1']);

CREATE TABLE t_bf_aj_expression
(
    id UInt64,
    tags Array(String),
    INDEX idx_expr arrayDistinct(tags) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_expression VALUES
    (0, ['other0', 'other0']),
    (1, ['target', 'target']),
    (2, ['other2']),
    (3, ['other3']),
    (4, ['other4']),
    (5, ['other5']),
    (6, ['other6']);

CREATE TABLE t_bf_aj_collision
(
    id UInt64,
    other Array(String),
    `arrayDistinct(other)` Array(String),
    INDEX idx_collision `arrayDistinct(other)` TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_collision VALUES (1, ['target'], []), (2, ['target'], ['target']);

CREATE TABLE t_bf_aj_deeper_collision
(
    id UInt64,
    other Array(String),
    `arrayDistinct(other)` Array(String),
    INDEX idx_deeper_collision arrayConcat(`arrayDistinct(other)`, ['suffix']) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_deeper_collision VALUES (1, ['target'], []), (2, ['target'], ['target']);

CREATE TABLE t_bf_aj_concat_expression
(
    id UInt64,
    tags Array(String),
    INDEX idx_concat_expression arrayConcat(arrayDistinct(tags), ['suffix']) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_concat_expression VALUES
    (0, []),
    (1, ['target']),
    (2, ['target', 'target']),
    (3, ['']),
    (4, ['01']),
    (5, ['1']),
    (6, ['other']);

CREATE TABLE t_bf_aj_physical_types
(
    id UInt64,
    dt DateTime('UTC'),
    dt64 DateTime64(3, 'UTC'),
    e Enum8('zero' = 0, 'one' = 1, 'two' = 2),
    INDEX idx_dt dt TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_dt64 dt64 TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_enum e TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_physical_types VALUES
    (1, toDateTime('2020-01-01 00:00:01', 'UTC'), toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC'), 'one'),
    (2, toDateTime('2020-01-02 00:00:02', 'UTC'), toDateTime64('2020-01-02 00:00:02.002', 3, 'UTC'), 'two');

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'bare GLOBAL IN prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE arrayJoin(tags) GLOBAL IN (SELECT value FROM t_bf_aj_target) SETTINGS query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'bare local IN literal prunes (supported control)', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE arrayJoin(tags) IN ('target') SETTINGS query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_expr') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayDistinct expression index remains supported', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_expression WHERE arrayJoin(arrayDistinct(tags)) IN ('target') SETTINGS query_plan_lower_array_join_function = 1, transform_null_in = 0);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'typed Dynamic identity GLOBAL IN prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(String))) GLOBAL IN (SELECT value FROM t_bf_aj_target) SETTINGS query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'emptyArrayToSingle excludes generated default and prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT value FROM t_bf_aj_target) SETTINGS query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'full generated predicate prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_target) SETTINGS query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'explicit inner ARRAY JOIN reaches Bloom pruning', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target));

-- Filter fusion into ARRAY JOIN is a plan-shape optimization and is skipped for serialized plans.
SELECT 'full wrapper PLAN retains globalIn element filter', countIf(position(explain, 'Element filter column: globalIn(') > 0) > 0 AND countIf(position(explain, 'ARRAY JOIN arrayJoin(emptyArrayToSingle(') > 0) > 0
FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0, compact = 0 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_target) SETTINGS query_plan_lower_array_join_function = 1, query_plan_fuse_filter_into_array_join = 1, serialize_query_plan = 0);

SELECT 'full nullable wrapper PLAN retains globalNullIn element filter', countIf(position(explain, 'Element filter column: globalNullIn(') > 0) > 0 AND countIf(position(explain, 'ARRAY JOIN arrayJoin(emptyArrayToSingle(') > 0) > 0
FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0, compact = 0 SELECT count() FROM t_bf_aj_wrappers WHERE globalNullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_nullable)) SETTINGS query_plan_lower_array_join_function = 1, query_plan_fuse_filter_into_array_join = 1, transform_null_in = 1, serialize_query_plan = 0);

SELECT 'explicit inner ARRAY JOIN PLAN composes source and globalIn filter', countIf(position(explain, 'ARRAY JOIN __array_join_exp_1') > 0) > 0 AND countIf(position(explain, 'Element filter column: globalIn(__array_join_exp_1,') > 0) > 0
FROM (EXPLAIN PLAN actions = 1, indexes = 1, pretty = 0, compact = 0 SELECT count() FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target) SETTINGS query_plan_fuse_filter_into_array_join = 1, serialize_query_plan = 0);

SELECT 'default-hit wrapper declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE arrayJoin(emptyArrayToSingle(tags)) GLOBAL IN (SELECT value FROM t_bf_aj_default) SETTINGS query_plan_lower_array_join_function = 1);

SELECT 'default-hit explicit clause declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default));

SELECT 'LEFT ARRAY JOIN declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers LEFT ARRAY JOIN tags AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default));

SELECT 'nullable generated NULL hit declines with transform_null_in=1', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') GLOBAL IN (SELECT value FROM t_bf_aj_nullable) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nullable generated NULL is absent from effective set with transform_null_in=0', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') GLOBAL IN (SELECT value FROM t_bf_aj_nullable) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nullIn NULL-free RHS prunes at transform_null_in=0', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE nullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_target)) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nullIn NULL-free RHS prunes at transform_null_in=1', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE nullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_target)) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nullIn NULL RHS prunes after transform_null_in=0 filters NULL', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE nullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_nullable)) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

SELECT 'nullIn effective NULL RHS declines at transform_null_in=1', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE nullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_nullable)) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'globalNullIn NULL-free RHS prunes at transform_null_in=0', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE globalNullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_target)) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'globalNullIn NULL-free RHS prunes at transform_null_in=1', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE globalNullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_target)) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'globalNullIn NULL RHS prunes after transform_null_in=0 filters NULL', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE globalNullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_nullable)) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

SELECT 'globalNullIn effective NULL RHS declines at transform_null_in=1', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE globalNullIn(CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)'), (SELECT value FROM t_bf_aj_nullable)) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

SELECT 'plain GLOBAL IN with NULL RHS declines Bloom pruning without emptyArrayToSingle', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') GLOBAL IN (SELECT value FROM t_bf_aj_nullable) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'plain GLOBAL IN NULL-free RHS prunes without emptyArrayToSingle', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') GLOBAL IN (SELECT value FROM t_bf_aj_nullable WHERE isNotNull(value)) SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'plain GLOBAL IN with NULL RHS preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable WHERE isNotNull(value))) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable WHERE isNotNull(value))) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'plain GLOBAL IN NULL-free RHS preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

SELECT 'type-changing Dynamic path declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_numeric WHERE arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) GLOBAL IN (SELECT value FROM t_bf_aj_uint8) SETTINGS query_plan_lower_array_join_function = 1);

SELECT 'tuple generated-default hit declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE tuple(arrayJoin(emptyArrayToSingle(tags)), id) IN (('', toUInt64(0)), ('target', toUInt64(1))) SETTINGS query_plan_lower_array_join_function = 1);

SELECT 'nested tuple NULL member declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST(NULL, 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2))) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tags') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'NULL-free nested Nullable tuple prunes idx_tags', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST('absent', 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2))) SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST(NULL, 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2)))) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST(NULL, 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2)))) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'NULL-bearing nested tuple preserves full tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')];

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST('absent', 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2)))) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE tuple(tuple(CAST(arrayJoin(CAST(tags AS Array(Nullable(String)))), 'Nullable(String)')), id) IN (tuple(tuple(CAST('absent', 'Nullable(String)')), toUInt64(3)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(1)), tuple(tuple(CAST('target', 'Nullable(String)')), toUInt64(2)))) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'NULL-free nested Nullable tuple preserves full tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')];

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(tags) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(tags) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'bare GLOBAL IN preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(String))) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(String))) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'typed Dynamic identity preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'full Dynamic-emptyArrayToSingle-Nullable wrapper preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(CAST(tags AS Dynamic) AS Array(String)))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT CAST(value, 'Nullable(String)') FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'full wrapper preserves generated empty-string default hit', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(0), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(3), toUInt8(0), '')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'emptyArrayToSingle no-default-hit preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'explicit inner ARRAY JOIN preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers ARRAY JOIN emptyArrayToSingle(tags) AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'explicit inner ARRAY JOIN default hit preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(0), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(3), toUInt8(0), '')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(arrayDistinct(tags)) AS value FROM t_bf_aj_expression WHERE value IN ('target')) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1, transform_null_in = 0) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(arrayDistinct(tags)) AS value FROM t_bf_aj_expression WHERE value IN ('target')) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1, transform_null_in = 0) AS full_scan
SELECT 'arrayDistinct expression index preserves duplicate semantics', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'emptyArrayToSingle default hit preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(0), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(3), toUInt8(0), '')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers LEFT ARRAY JOIN tags AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, value FROM t_bf_aj_wrappers LEFT ARRAY JOIN tags AS value WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_default)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'LEFT ARRAY JOIN preserves padded-default tuple multiset', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(0), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(3), toUInt8(0), '')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'nullable default matches NULL only with transform_null_in=1', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(1), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
SETTINGS transform_null_in = 1, query_plan_lower_array_join_function = 1;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'nullable default is not a match with transform_null_in=0', indexed = full_scan AND indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]
SETTINGS transform_null_in = 0, query_plan_lower_array_join_function = 1;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS nullin_free_0_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS nullin_free_0_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS nullin_free_1_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS nullin_free_1_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS nullin_null_0_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS nullin_null_0_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS nullin_null_1_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE nullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS nullin_null_1_full
SELECT arrayJoin([
    tuple('NULL-free RHS, transform_null_in=0', nullin_free_0_indexed = nullin_free_0_full AND nullin_free_0_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('NULL-free RHS, transform_null_in=1', nullin_free_1_indexed = nullin_free_1_full AND nullin_free_1_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('RHS with NULL, transform_null_in=0', nullin_null_0_indexed = nullin_null_0_full AND nullin_null_0_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('effective NULL RHS, transform_null_in=1', nullin_null_1_indexed = nullin_null_1_full AND nullin_null_1_indexed = [(toUInt64(0), toUInt8(1), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')])
]) AS result
ORDER BY result.1;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS global_nullin_free_0_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS global_nullin_free_0_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS global_nullin_free_1_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_target)) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS global_nullin_free_1_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS global_nullin_null_0_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, transform_null_in = 0, query_plan_lower_array_join_function = 1)) AS global_nullin_null_0_full,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 1, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS global_nullin_null_1_indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, CAST(arrayJoin(emptyArrayToSingle(CAST(tags AS Array(Nullable(String))))), 'Nullable(String)') AS value FROM t_bf_aj_wrappers WHERE globalNullIn(value, (SELECT value FROM t_bf_aj_nullable)) SETTINGS use_skip_indexes = 0, transform_null_in = 1, query_plan_lower_array_join_function = 1)) AS global_nullin_null_1_full
SELECT arrayJoin([
    tuple('NULL-free RHS, transform_null_in=0', global_nullin_free_0_indexed = global_nullin_free_0_full AND global_nullin_free_0_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('NULL-free RHS, transform_null_in=1', global_nullin_free_1_indexed = global_nullin_free_1_full AND global_nullin_free_1_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('RHS with NULL, transform_null_in=0', global_nullin_null_0_indexed = global_nullin_null_0_full AND global_nullin_null_0_indexed = [(toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')]),
    tuple('effective NULL RHS, transform_null_in=1', global_nullin_null_1_indexed = global_nullin_null_1_full AND global_nullin_null_1_indexed = [(toUInt64(0), toUInt8(1), ''), (toUInt64(1), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target'), (toUInt64(2), toUInt8(0), 'target')])
]) AS result
ORDER BY result.1;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), toString(value)))) FROM (SELECT id, arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) AS value FROM t_bf_aj_numeric WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_uint8)) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), toString(value)))) FROM (SELECT id, arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) AS value FROM t_bf_aj_numeric WHERE value GLOBAL IN (SELECT value FROM t_bf_aj_uint8)) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'type-changing Dynamic preserves converted values', indexed = full_scan AND indexed = [(toUInt64(4), toUInt8(0), '1'), (toUInt64(5), toUInt8(0), '1')]
;

WITH
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE tuple(value, id) IN (('', toUInt64(0)), ('target', toUInt64(1)))) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, isNull(value), ifNull(value, '')))) FROM (SELECT id, arrayJoin(emptyArrayToSingle(tags)) AS value FROM t_bf_aj_wrappers WHERE tuple(value, id) IN (('', toUInt64(0)), ('target', toUInt64(1)))) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1) AS full_scan
SELECT 'tuple default hit preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(0), toUInt8(0), ''), (toUInt64(1), toUInt8(0), 'target')]
;

CREATE TABLE t_bf_aj_invalid
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_aj_invalid VALUES (0, ['bad']);

SELECT 'invalid Dynamic conversion EXPLAIN declines Bloom pruning', max(explain LIKE '%Name: idx_tags%') = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_aj_invalid WHERE arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) GLOBAL IN (SELECT value FROM t_bf_aj_uint8) SETTINGS query_plan_lower_array_join_function = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_collision ARRAY JOIN arrayDistinct(other) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_collision ARRAY JOIN arrayDistinct(other) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS full_scan
SELECT 'direct physical/FUNCTION collision preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), 'target'), (toUInt64(2), 'target')];

SELECT 'direct physical/FUNCTION collision declines Bloom index', max(explain LIKE '%Name: idx_collision%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_aj_collision ARRAY JOIN arrayDistinct(other) AS value WHERE value IN ('target') SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, toNullable(arrayJoin(arrayDistinct(other))) AS value FROM t_bf_aj_collision WHERE value IN ('target')) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, toNullable(arrayJoin(arrayDistinct(other))) AS value FROM t_bf_aj_collision WHERE value IN ('target')) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS full_scan
SELECT 'wrapped physical/FUNCTION collision preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), toNullable('target')), (toUInt64(2), toNullable('target'))];

SELECT 'wrapped physical/FUNCTION collision declines Bloom index', max(explain LIKE '%Name: idx_collision%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, toNullable(arrayJoin(arrayDistinct(other))) AS value FROM t_bf_aj_collision WHERE value IN ('target') SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_deeper_collision ARRAY JOIN arrayConcat(arrayDistinct(other), ['suffix']) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_deeper_collision ARRAY JOIN arrayConcat(arrayDistinct(other), ['suffix']) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS full_scan
SELECT 'deeper INPUT/FUNCTION child collision preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), 'target'), (toUInt64(2), 'target')];

SELECT 'deeper INPUT/FUNCTION child collision declines Bloom index', max(explain LIKE '%Name: idx_deeper_collision%') = 0
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_aj_deeper_collision ARRAY JOIN arrayConcat(arrayDistinct(other), ['suffix']) AS value WHERE value IN ('target') SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_concat_expression ARRAY JOIN arrayConcat(arrayDistinct(tags), ['suffix']) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS indexed,
    (SELECT arraySort(groupArray(tuple(id, value))) FROM (SELECT id, value FROM t_bf_aj_concat_expression ARRAY JOIN arrayConcat(arrayDistinct(tags), ['suffix']) AS value WHERE value IN ('target')) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1) AS full_scan
SELECT 'arrayConcat expression index preserves tuple multiset', indexed = full_scan AND indexed = [(toUInt64(1), 'target'), (toUInt64(2), 'target')];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_concat_expression') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayConcat expression index still prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT id, value FROM t_bf_aj_concat_expression ARRAY JOIN arrayConcat(arrayDistinct(tags), ['suffix']) AS value WHERE value IN ('target') SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'physical DateTime IN still prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_aj_physical_types WHERE dt IN (toDateTime('2020-01-01 00:00:01', 'UTC')));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_dt64') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'physical DateTime64 IN still prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_aj_physical_types WHERE dt64 IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')));

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_enum') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    name_row > 0 AS has_index,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'physical Enum IN still prunes', has_index AND selected > 0 AND selected < total
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_aj_physical_types WHERE e IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE dt IN (toDateTime('2020-01-01 00:00:01', 'UTC')) SETTINGS use_skip_indexes = 1) AS indexed,
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE dt IN (toDateTime('2020-01-01 00:00:01', 'UTC')) SETTINGS use_skip_indexes = 0) AS full_scan
SELECT 'physical DateTime IN preserves results', indexed = full_scan AND indexed = [toUInt64(1)];

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE dt64 IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')) SETTINGS use_skip_indexes = 1) AS indexed,
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE dt64 IN (toDateTime64('2020-01-01 00:00:01.001', 3, 'UTC')) SETTINGS use_skip_indexes = 0) AS full_scan
SELECT 'physical DateTime64 IN preserves results', indexed = full_scan AND indexed = [toUInt64(1)];

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE e IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))) SETTINGS use_skip_indexes = 1) AS indexed,
    (SELECT arraySort(groupArray(id)) FROM t_bf_aj_physical_types WHERE e IN (CAST('one' AS Enum8('zero' = 0, 'one' = 1, 'two' = 2))) SETTINGS use_skip_indexes = 0) AS full_scan
SELECT 'physical Enum IN preserves results', indexed = full_scan AND indexed = [toUInt64(1)];

SELECT count() FROM t_bf_aj_invalid WHERE arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) GLOBAL IN (SELECT value FROM t_bf_aj_uint8) SETTINGS use_skip_indexes = 1, query_plan_lower_array_join_function = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT count() FROM t_bf_aj_invalid WHERE arrayJoin(CAST(CAST(tags AS Dynamic) AS Array(UInt8))) GLOBAL IN (SELECT value FROM t_bf_aj_uint8) SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE t_bf_aj_invalid;
DROP TABLE t_bf_aj_concat_expression;
DROP TABLE t_bf_aj_deeper_collision;
DROP TABLE t_bf_aj_collision;
DROP TABLE t_bf_aj_physical_types;
DROP TABLE t_bf_aj_expression;
DROP TABLE t_bf_aj_numeric;
DROP TABLE t_bf_aj_uint8;
DROP TABLE t_bf_aj_nullable;
DROP TABLE t_bf_aj_default;
DROP TABLE t_bf_aj_target;
DROP TABLE t_bf_aj_wrappers;
