DROP TABLE IF EXISTS json_bf_structural_null;

CREATE TABLE json_bf_structural_null
(
    id UInt64,
    j JSON(
        needle String,
        array_nullable Array(Nullable(Int64)),
        map_nullable Map(String, Nullable(Int64)),
        unsupported Variant(UInt64, String)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_structural_null FORMAT JSONEachRow
{"id":1,"j":{"needle":"one","array_nullable":[null,1],"map_nullable":{"x":null},"unsupported":1}}
{"id":2,"j":{"needle":"two","array_nullable":[2],"map_nullable":{"x":3},"unsupported":"two"}}
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

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_bf_structural_null WHERE j.needle = 'missing'
    SETTINGS force_data_skipping_indices = 'idx'
)
WHERE trim(explain) = 'Granules: 0/2';

DROP TABLE json_bf_structural_null;
