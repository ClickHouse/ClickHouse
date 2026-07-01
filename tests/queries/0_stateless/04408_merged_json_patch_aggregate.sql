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
    UNION ALL
    SELECT '{"a":{"x":1}}'::JSON, 1
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

