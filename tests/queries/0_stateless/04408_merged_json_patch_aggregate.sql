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
-- Without the fix, iterating the row yields both a=42 and a.b=0; conflict resolution erases a.
SELECT toJSONString(mergedJSONPatch(patch, version))
FROM (
    SELECT '{\"a\":42}'::JSON(a UInt32, `a.b` UInt32) AS patch, 1 AS version
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

