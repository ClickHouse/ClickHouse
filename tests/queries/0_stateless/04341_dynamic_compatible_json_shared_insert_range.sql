SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src
(
    id UInt64,
    d Dynamic(max_types=1)
)
ENGINE = Memory;

CREATE TABLE dst
(
    id UInt64,
    d Dynamic(max_types=2)
)
ENGINE = Memory;

INSERT INTO dst VALUES
(0, CAST(CAST('{"a":0,"b":0}' AS JSON(max_dynamic_paths=1)) AS Dynamic(max_types=2)));

INSERT INTO src
SELECT
    number + 1,
    multiIf(
        number = 0,
        CAST(42 AS UInt64),
        CAST('{"a":1,"b":10}' AS JSON(max_dynamic_paths=0)))::Dynamic(max_types=1)
FROM numbers(2);

INSERT INTO dst SELECT * FROM src ORDER BY id;

SELECT
    id,
    dynamicType(d),
    isDynamicElementInSharedData(d),
    d.JSON.a.:Int64,
    d.JSON.b.:Int64
FROM dst
ORDER BY id
FORMAT TSVRaw;

DROP TABLE src;
DROP TABLE dst;
