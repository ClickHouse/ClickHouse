DROP TABLE IF EXISTS json_bf_structural_null;

CREATE TABLE json_bf_structural_null
(
    id UInt64,
    j JSON(
        needle String,
        array_nullable Array(Nullable(Int64)),
        map_nullable Map(String, Nullable(Int64)),
        unsupported Variant(UInt64, String),
        dynamic_value Dynamic,
        payload JSON(
            v Variant(UInt64, String),
            n Nullable(Int64),
            arr Array(Int64)),
        array_payload Array(JSON(v Variant(UInt64, String))),
        map_tuple Map(String, Tuple(a Int64)),
        nested Tuple(
            value Variant(UInt64, String),
            complex Variant(Tuple(a Int64), String))),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_structural_null FORMAT JSONEachRow
{"id":1,"j":{"needle":"one","array_nullable":[null,1],"map_nullable":{"x":null},"unsupported":1,"dynamic_value":"one","payload":{"v":1,"n":null,"arr":[1],"ordinary":{"String":"one"}},"array_payload":[{"v":"aaa"}],"map_tuple":{"k":{"a":1}},"nested":{"value":1,"complex":{"a":1}}}}
{"id":2,"j":{"needle":"two","array_nullable":[2],"map_nullable":{"x":3},"unsupported":"two","dynamic_value":"two","payload":{"v":"two","n":2,"arr":[1,2],"ordinary":{"String":"two"}},"array_payload":[{"v":"bbb"}],"map_tuple":{"k":{"a":2}},"nested":{"value":"two","complex":{"a":2}}}}
;

SELECT 'array null', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE has(j.array_nullable.null, 1) AND j.needle = 'one'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'map value null', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE has(j.map_nullable.values.null, 1) AND j.needle = 'one'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'variant conservative', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.unsupported = 'two' AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'dynamic type subcolumn', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.dynamic_value.String = 'two' AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'variant type subcolumn', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.nested.value.String = 'two' AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'variant nested type subcolumn', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.nested.complex.`Tuple(a Int64)`.a = 2 AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested JSON String field', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.payload.ordinary.String = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested JSON variant subcolumn', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.payload.v.String = 'two' AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested JSON nullable null', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.payload.n.null = 1 AND j.needle = 'one'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'nested JSON array size', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE j.payload.arr.size0 = 2 AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'array JSON variant subcolumn', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE has(j.array_payload[].v.String, 'bbb') AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT 'map tuple values descendant', arraySort(groupArray(id))
FROM json_bf_structural_null
WHERE has(j.map_tuple.values.a, 2) AND j.needle = 'two'
SETTINGS force_data_skipping_indices = 'idx';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_bf_structural_null WHERE j.needle = 'missing'
    SETTINGS force_data_skipping_indices = 'idx'
)
WHERE trim(explain) = 'Granules: 0/2';

DROP TABLE json_bf_structural_null;
