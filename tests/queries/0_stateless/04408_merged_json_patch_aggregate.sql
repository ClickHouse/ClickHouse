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

