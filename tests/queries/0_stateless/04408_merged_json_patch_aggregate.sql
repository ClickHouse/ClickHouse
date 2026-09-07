SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":{"x":1}}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":{"y":2}}'::JSON, 2
);

SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":{"x":1,"y":2}}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":5}'::JSON, 2
);

SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":5}'::JSON AS patch, 2 AS version
);

SELECT toJSONString(mergedJSONPatchMerge(state))
FROM
(
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM
    (
        SELECT '{"a":{"x":1}}'::JSON AS patch, 1 AS version
        UNION ALL
        SELECT '{"a":{"y":2}}'::JSON, 2
    )
);

SELECT toJSONString(mergedJSONPatchMerge(state))
FROM
(
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM
    (
        SELECT '{"a":{"x":1}}'::JSON AS patch, 2 AS version
        UNION ALL
        SELECT '{"a":5}'::JSON, 1
    )
);

SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"arr":["1",{"k":1}]}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"arr":["2",{"k":2}]}'::JSON, 2
);

-- Mixed array via State+Merge combinator: object elements inside arrays must not be stringified.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM
(
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM
    (
        SELECT '{"arr":["1",{"k":1}]}'::JSON AS patch, 1 AS version
        UNION ALL
        SELECT '{"arr":["2",{"k":2}]}'::JSON, 2
    )
);

-- Typed path with conflicting sibling: a typed "a.b" path at default must not clobber a non-default "a".
-- Without the fix, intra-row conflict resolution sees a.b (same sort key) and erases a=42.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{\"a\":42}'::JSON(a UInt32, `a.b` UInt32) AS patch, 1 AS version
);

-- Same case via State+Merge: mergedJSONPatchMerge must agree with direct aggregation.
-- Without the fix, merge() replayed entries one-by-one and a.b=0 erased a=42 in the merged state.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT '{\"a\":42}'::JSON(a UInt32, `a.b` UInt32) AS patch, 1 AS version
    )
);

-- Multi-row typed-path case: older {a:5} then newer {a:42}. The newer row wins and both typed
-- paths (a and a.b) must survive in the result, matching direct toJSONString on the newer row.
-- Without the phase-2/3 split, pushLeafEntry inserted the new "a" at a sorted position inside
-- the pre-existing prefix; the next survivor's erase pass mis-identified it as a pre-existing
-- entry and erased it, producing a=0 (column default) instead of a=42.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{\"a\":5}'::JSON(a UInt32, `a.b` UInt32) AS patch, 1 AS version
    UNION ALL
    SELECT '{\"a\":42}'::JSON(a UInt32, `a.b` UInt32), 2
);

SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState('{\"a\":5}'::JSON(a UInt32, `a.b` UInt32), 1) AS state
    UNION ALL
    SELECT mergedJSONPatchState('{\"a\":42}'::JSON(a UInt32, `a.b` UInt32), 2)
);

-- Typed path at default value (zero) written explicitly must still win over an older non-zero value.
-- Without the fix, a genuine {"a":0} (newer) was silently dropped and {"a":5} (older) survived.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{\"a\":5}'::JSON(a UInt32) AS patch, 1 AS version
    UNION ALL
    SELECT '{\"a\":0}'::JSON(a UInt32), 2
);

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT '{\"a\":5}'::JSON(a UInt32) AS patch, 1 AS version
        UNION ALL
        SELECT '{\"a\":0}'::JSON(a UInt32), 2
    )
);

-- Known limitation: ColumnObject drops empty-object paths, so {"a":{}} cannot replace an older
-- scalar at "a". The result is {"a":5} instead of the RFC 7396-correct {"a":{}}.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":5}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":{}}'::JSON, 2
); -- expected by RFC 7396: {"a":{}}, actual: {"a":5}

-- Nullable typed path: later patch omits the path (NULL = absent).
-- The older non-null value must survive because RFC 7396 says an absent member is unchanged.
-- With JSON(a Nullable(UInt32)), absence is stored as NULL, which the aggregate skips.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":5}'::JSON(a Nullable(UInt32)) AS patch, 1 AS version
    UNION ALL
    SELECT '{}'::JSON(a Nullable(UInt32)), 2
);

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM
(
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM
    (
        SELECT '{"a":5}'::JSON(a Nullable(UInt32)) AS patch, 1 AS version
        UNION ALL
        SELECT '{}'::JSON(a Nullable(UInt32)), 2
    )
);

-- Nullable typed path: newer patch explicitly writes the default value (0).
-- 0 is a genuine write and must win over the older 5.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":5}'::JSON(a Nullable(UInt32)) AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":0}'::JSON(a Nullable(UInt32)), 2
);

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM
(
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM
    (
        SELECT '{"a":5}'::JSON(a Nullable(UInt32)) AS patch, 1 AS version
        UNION ALL
        SELECT '{"a":0}'::JSON(a Nullable(UInt32)), 2
    )
);

-- Known limitation: non-Nullable typed path. With JSON(a UInt32), a row that omits "a" is
-- indistinguishable from a row that explicitly writes "a":0. The aggregate therefore cannot
-- preserve the older non-zero value when the newer row omits the path.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":5}'::JSON(a UInt32) AS patch, 1 AS version
    UNION ALL
    SELECT '{}'::JSON(a UInt32), 2
); -- known limitation: produces {"a":0} instead of {"a":5}

DROP TABLE IF EXISTS merged_json_patch_states;

CREATE TABLE merged_json_patch_states
(
    id UInt8,
    state AggregateFunction(mergedJSONPatch, JSON, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

INSERT INTO merged_json_patch_states
SELECT
    1 AS id,
    mergedJSONPatchState(patch, version) AS state
FROM
(
    SELECT '{"a":{"x":1}}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":{"y":2}}'::JSON, 2
);

INSERT INTO merged_json_patch_states
SELECT
    2 AS id,
    mergedJSONPatchState(patch, version) AS state
FROM
(
    SELECT '{"a":{"x":1,"y":2}}'::JSON AS patch, 1 AS version
    UNION ALL
    SELECT '{"a":5}'::JSON, 2
);

SELECT
    id,
    toJSONString(finalizeAggregation(state))
FROM merged_json_patch_states
ORDER BY id;

OPTIMIZE TABLE merged_json_patch_states FINAL;

SELECT
    id,
    toJSONString(finalizeAggregation(state))
FROM merged_json_patch_states
ORDER BY id;

DROP TABLE merged_json_patch_states;

-- runningAccumulate must not null-deref the arena: allocatesMemoryInArena() must return true
-- so that runningAccumulate creates a real Arena instead of passing nullptr.
-- Each row is a self-contained single-row aggregate state; runningAccumulate merges them
-- left-to-right and emits the accumulated result after each row.
SET allow_deprecated_error_prone_window_functions = 1;
SELECT toJSONString(runningAccumulate(state))
FROM
(
    SELECT rn, mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT 1 AS rn, '{"a":1}'::JSON AS patch, 1 AS version
        UNION ALL SELECT 2, '{"a":2}'::JSON, 2
        UNION ALL SELECT 3, '{"b":3}'::JSON, 3
    )
    GROUP BY rn, version
    ORDER BY rn
);


-- Map typed path: typed Map/JSON paths are atomic — the whole value is replaced by the newer
-- patch. There is no deep merge inside a typed Map or JSON path.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":1,"y":2}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"y":3}}'::JSON(a Map(String, UInt32)), 2
); -- newer patch replaces the whole map: {"a":{"y":3}}

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT '{"a":{"x":1,"y":2}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version
        UNION ALL SELECT '{"a":{"y":3}}'::JSON(a Map(String, UInt32)), 2
    )
);

-- Map typed path: newer non-Map scalar at the same path replaces the whole key.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":1}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version
    UNION ALL SELECT '{"a":5}'::JSON, 2
);

-- JSON typed path: single-row round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":1}}'::JSON(a JSON) AS patch, 1 AS version
);

-- JSON typed path: newer row replaces the whole value atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":1,"y":2}}'::JSON(a JSON) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"y":3}}'::JSON(a JSON), 2
); -- newer patch replaces the whole JSON value: {"a":{"y":3}}

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT '{"a":{"x":1,"y":2}}'::JSON(a JSON) AS patch, 1 AS version
        UNION ALL SELECT '{"a":{"y":3}}'::JSON(a JSON), 2
    )
);

-- Nullable(JSON) typed path: single-row round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x":1}}'::JSON(a Nullable(JSON)) AS patch, 1 AS version);

-- Nullable(JSON) typed path: newer row wins atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":1,"y":2}}'::JSON(a Nullable(JSON)) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"y":3}}'::JSON(a Nullable(JSON)), 2
);

-- Nested Map typed path: single-row round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x":{"y":1,"z":2}}}'::JSON(a Map(String, Map(String, UInt32))) AS patch, 1 AS version);

-- Nested Map typed path: newer row replaces the whole map atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x":{"y":1,"z":2}}}'::JSON(a Map(String, Map(String, UInt32))) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"x":{"z":99}}}'::JSON(a Map(String, Map(String, UInt32))), 2
);

-- Nested Map typed path: two first-level keys, single-row round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x":{"y":1},"z":{"w":2}}}'::JSON(a Map(String, Map(String, UInt32))) AS patch, 1 AS version);

-- JSON(a JSON, `a.b` UInt32): both typed paths must be written independently.
-- Note: toJSONString renders `a JSON` and `a.b UInt32` as two separate "a" entries
-- (a property of the overlapping typed-path schema, observable on the raw input row too).
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x":1,"b":42}}'::JSON(a JSON, `a.b` UInt32) AS patch, 1 AS version);

-- Map(String, JSON) typed path: single-row round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"k":{"x":1}}}'::JSON(a Map(String, JSON)) AS patch, 1 AS version);

-- Map(String, JSON) typed path: newer row replaces the whole map atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"k":{"x":1,"y":2}}}'::JSON(a Map(String, JSON)) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"k":{"y":3}}}'::JSON(a Map(String, JSON)), 2
);

-- Map typed path: dotted key round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x.y":1}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version);

-- Map typed path with dotted key: newer row replaces the whole map atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{"a":{"x.y":1,"z":2}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version
    UNION ALL SELECT '{"a":{"z":3}}'::JSON(a Map(String, UInt32)), 2
);

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT '{"a":{"x.y":1,"z":2}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version
        UNION ALL SELECT '{"a":{"z":3}}'::JSON(a Map(String, UInt32)), 2
    )
);

-- Map key with literal \x01 character: both keys survive in a single row.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (SELECT '{"a":{"x\u0001y":1,"x.y":2}}'::JSON(a Map(String, UInt32)) AS patch, 1 AS version);

-- Type preservation: inferred Date path must round-trip as Date, not be re-derived as UInt16.
-- The bug: going through Field for typed paths loses the declared DataType (Date{18262} becomes
-- Field(UInt16{18262})), and re-deriving the type on output yields UInt16 instead of Date.
WITH CAST('{"d" : "2020-01-01"}', 'JSON') AS json
SELECT
    JSONAllPathsWithTypes(mergedJSONPatch(json, 1)) AS merged_paths_and_types;

-- Same case via State+Merge combinator.
SELECT
    JSONAllPathsWithTypes(mergedJSONPatchMerge(state)) AS merged_paths_and_types
FROM (
    SELECT mergedJSONPatchState(CAST('{"d" : "2020-01-01"}', 'JSON'), 1) AS state
);

-- Typed Date path must also round-trip correctly (JSON(d Date)).
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT CAST('{"d":"2020-01-01"}', 'JSON(d Date)') AS patch, 1 AS version
    UNION ALL
    SELECT CAST('{"d":"2021-06-15"}', 'JSON(d Date)'), 2
);

-- Same case via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, version) AS state
    FROM (
        SELECT CAST('{"d":"2020-01-01"}', 'JSON(d Date)') AS patch, 1 AS version
        UNION ALL
        SELECT CAST('{"d":"2021-06-15"}', 'JSON(d Date)'), 2
    )
);

-- Typed DateTime path round-trip.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT CAST('{"ts":"2020-01-01 12:00:00"}', 'JSON(ts DateTime)') AS patch, 1 AS version
    UNION ALL
    SELECT CAST('{"ts":"2021-06-15 08:30:00"}', 'JSON(ts DateTime)'), 2
);

-- Nullable sort key: null < any non-null value in ClickHouse Field ordering.
-- The non-null row (version=2) must win; the null-keyed row must lose.
SELECT toJSONString(mergedJSONPatch(patch, toNullable(version)))
FROM (
    SELECT '{"a":1}'::JSON AS patch, toInt32(NULL) AS version
    UNION ALL
    SELECT '{"a":2}'::JSON, 2
);

-- Nullable sort key with multiple paths: a null-keyed patch must not erase entries
-- that came from a non-null key, even when the null-keyed patch names the same path.
SELECT toJSONString(mergedJSONPatch(patch, toNullable(version)))
FROM (
    SELECT '{"a":1,"b":1}'::JSON AS patch, toInt32(2) AS version
    UNION ALL
    SELECT '{"a":99}'::JSON, toInt32(NULL)
);

-- Variant-typed path: must not throw NOT_IMPLEMENTED (Field overload is unimplemented for Variant).
SELECT toJSONString(mergedJSONPatch(patch, 1))
FROM (SELECT CAST('{"a":1}', 'JSON(a Variant(UInt64, String))') AS patch);

-- Variant-typed path via State+Merge combinator.
SELECT toJSONString(mergedJSONPatchMerge(state))
FROM (
    SELECT mergedJSONPatchState(patch, 1) AS state
    FROM (SELECT CAST('{"a":1}', 'JSON(a Variant(UInt64, String))') AS patch)
);

-- Tuple-typed path: newer version wins atomically.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT CAST('{"t":{"x":1,"y":2}}', 'JSON(t Tuple(x UInt32, y UInt32))') AS patch, 1 AS version
    UNION ALL
    SELECT CAST('{"t":{"x":10,"y":20}}', 'JSON(t Tuple(x UInt32, y UInt32))'), 2
);

-- Composite and Variant/Dynamic sort keys must be rejected at registration time.
CREATE TABLE t_bad_sort_keys (json JSON, v Variant(UInt64, String), d Dynamic, t Tuple(UInt32, UInt32), a Array(UInt32), m Map(String, UInt32)) ENGINE=Memory;
SELECT mergedJSONPatch(json, v) FROM t_bad_sort_keys; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergedJSONPatch(json, d) FROM t_bad_sort_keys; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergedJSONPatch(json, t) FROM t_bad_sort_keys; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergedJSONPatch(json, a) FROM t_bad_sort_keys; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergedJSONPatch(json, m) FROM t_bad_sort_keys; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- DateTime64 sort keys must read correctly from ColumnDecimal (not cast to ColumnVector).
-- Regression test for: https://github.com/ClickHouse/ClickHouse/issues/[issue-number]
-- Without the fix, DateTime64 keys would incorrectly cast ColumnDecimal to ColumnVector, causing UB in release builds.
SELECT toJSONString(mergedJSONPatch(patch, ts))
FROM
(
    SELECT '{"a":1}'::JSON AS patch, toDateTime64('2020-01-01 00:00:00', 3) AS ts
    UNION ALL
    SELECT '{"a":2}'::JSON, toDateTime64('2020-01-02 00:00:00', 3)
);

-- Float64 sort keys: NaN is treated as minimum (same as argMax semantics).
-- A row with a finite version always wins over a NaN version; expected: {"a":1}.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":1}'::JSON AS patch, 1.0::Float64 AS version
    UNION ALL
    SELECT '{"a":2}'::JSON, nan::Float64
);

-- Nullable(Float64) sort keys: NaN is also treated as minimum via KeyGeneric path.
-- Expected: {"a":1}.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":1}'::JSON AS patch, 1.0::Nullable(Float64) AS version
    UNION ALL
    SELECT '{"a":2}'::JSON, nan::Nullable(Float64)
);

-- Nullable(Float64) with NULL: NULL sorts as minimum (Field::Null < Field::Float64), finite key wins.
-- Expected: {"a":1}.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM
(
    SELECT '{"a":1}'::JSON AS patch, 1.0::Nullable(Float64) AS version
    UNION ALL
    SELECT '{"a":2}'::JSON, CAST(NULL, 'Nullable(Float64)')
);

-- LEFT JOIN regression test: columns from the right side of a LEFT JOIN become ColumnNullable at runtime.
-- KeyFixed and KeyString must handle ColumnNullable, preserving is_null so NULL sorts below any non-null key.
SET join_use_nulls = 1;

CREATE TABLE t_left_json (id UInt32, patch String) ENGINE = Memory;
CREATE TABLE t_right_keys (id UInt32, ver_num UInt32, ver_str String) ENGINE = Memory;

INSERT INTO t_left_json VALUES (1, '{"a":1}'), (2, '{"b":2}');
INSERT INTO t_right_keys VALUES (1, 10, 'v10');

SELECT toJSONString(mergedJSONPatch(CAST(patch, 'JSON'), t_right_keys.ver_num))
FROM t_left_json LEFT JOIN t_right_keys ON t_left_json.id = t_right_keys.id;

SELECT toJSONString(mergedJSONPatch(CAST(patch, 'JSON'), t_right_keys.ver_str))
FROM t_left_json LEFT JOIN t_right_keys ON t_left_json.id = t_right_keys.id;

DROP TABLE t_left_json;
DROP TABLE t_right_keys;

-- Outer join test where matched row has key 0 / '' and unmatched row gets NULL.
-- Key 0 and '' must beat NULL.
CREATE TABLE t_left_comp (id UInt32, patch String) ENGINE = Memory;
CREATE TABLE t_right_comp (id UInt32, ver_num UInt32, ver_str String) ENGINE = Memory;

INSERT INTO t_left_comp VALUES (1, '{"a":1}'), (2, '{"a":2}');
INSERT INTO t_right_comp VALUES (1, 0, '');

SELECT toJSONString(mergedJSONPatch(CAST(patch, 'JSON'), t_right_comp.ver_num))
FROM t_left_comp LEFT JOIN t_right_comp ON t_left_comp.id = t_right_comp.id;

SELECT toJSONString(mergedJSONPatch(CAST(patch, 'JSON'), t_right_comp.ver_str))
FROM t_left_comp LEFT JOIN t_right_comp ON t_left_comp.id = t_right_comp.id;

DROP TABLE t_left_comp;
DROP TABLE t_right_comp;

-- Outer join test where patch column itself comes from the nullable side of a LEFT JOIN.
-- The NULL patch from unmatched rows must be ignored without crashing (e.g. Bad cast from ColumnNullable to ColumnObject).
CREATE TABLE t_left_base (id UInt32) ENGINE = Memory;
CREATE TABLE t_right_patches (id UInt32, patch JSON, ver UInt32) ENGINE = Memory;

INSERT INTO t_left_base VALUES (1), (2);
INSERT INTO t_right_patches VALUES (1, '{"x":100}', 1);

SELECT toJSONString(mergedJSONPatch(t_right_patches.patch, t_right_patches.ver))
FROM t_left_base LEFT JOIN t_right_patches ON t_left_base.id = t_right_patches.id;

DROP TABLE t_left_base;
DROP TABLE t_right_patches;

DROP TABLE t_bad_sort_keys;
