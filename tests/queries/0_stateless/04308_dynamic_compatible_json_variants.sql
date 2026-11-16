SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;

CREATE TABLE test
(
    id UInt64,
    d Dynamic
)
ENGINE = Memory;

INSERT INTO test
SELECT 1, CAST(CAST('{"a":1,"b":10}' AS JSON(max_dynamic_paths=0)) AS Dynamic)
UNION ALL
SELECT 2, CAST(CAST('{"a":2,"b":20}' AS JSON(max_dynamic_paths=1)) AS Dynamic);

SELECT
    id,
    d.JSON.a.:Int64,
    d.JSON.b.:Int64
FROM test
ORDER BY id
FORMAT TSVRaw;
