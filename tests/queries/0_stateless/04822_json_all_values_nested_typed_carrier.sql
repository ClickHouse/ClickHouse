DROP TABLE IF EXISTS json_all_values_nested_typed_carrier;

CREATE TABLE json_all_values_nested_typed_carrier
(
    data JSON,
    INDEX idx JSONAllValues(data) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_nested_typed_carrier VALUES ('{"arr":[{"b":42}]}');

SELECT count() FROM json_all_values_nested_typed_carrier WHERE data.arr[].b.:Int64 = [42];
SELECT count() FROM json_all_values_nested_typed_carrier WHERE data.arr[].b.:Int64 IN ([42]);

DROP TABLE json_all_values_nested_typed_carrier;
