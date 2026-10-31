SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_shared;

CREATE TABLE test
(
    id UInt64,
    d Dynamic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO test VALUES (1, CAST(CAST('{"a":1,"b":10}' AS JSON(max_dynamic_paths=0)) AS Dynamic));
INSERT INTO test VALUES (2, CAST(CAST('{"a":2,"b":20}' AS JSON(max_dynamic_paths=1)) AS Dynamic));

SELECT
    id,
    dynamicType(d),
    d.JSON.a.:Int64,
    d.JSON.b.:Int64
FROM test
ORDER BY id
FORMAT TSVRaw;

DROP TABLE test;

CREATE TABLE test_shared
(
    id UInt64,
    d Dynamic(max_types=1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_shared
SELECT
    number,
    multiIf(
        number = 0,
        CAST('{"a":1,"b":10}' AS JSON(max_dynamic_paths=0)),
        number = 1,
        CAST(42 AS UInt64),
        CAST('{"a":2,"b":20}' AS JSON(max_dynamic_paths=1)))::Dynamic(max_types=1)
FROM numbers(3);

SELECT
    id,
    dynamicType(d),
    isDynamicElementInSharedData(d),
    d.JSON.a.:Int64,
    d.JSON.b.:Int64
FROM test_shared
ORDER BY id
FORMAT TSVRaw;

DROP TABLE test_shared;
