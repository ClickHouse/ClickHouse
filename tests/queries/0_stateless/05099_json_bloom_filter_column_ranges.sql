DROP TABLE IF EXISTS json_bf_ranges;
DROP TABLE IF EXISTS json_bf_blocks;
CREATE TABLE json_bf_ranges
(
    id UInt64,
    j JSON(z Float32, nz Nullable(Float64), arr Array(Nullable(Int64))),
    INDEX idx j TYPE jsonbf_v1() GRANULARITY 2
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO json_bf_ranges
SELECT number, ('{"v":' || multiIf(number % 3 = 0, toString(number), number % 3 = 1, '"' || toString(number) || '"', 'null')
    || ',"arr":[' || toString(number) || ',' || toString(number + 100) || ',null]'
    || ',"z":' || if(number % 2 = 0, '-0.0', '1.0')
    || ',"nz":' || if(number % 3 = 0, 'null', '-0.0')
    || if(number % 5 = 0, ',"sparse":"hit-' || toString(number) || '"', '') || '}')::JSON(z Float32, nz Nullable(Float64), arr Array(Nullable(Int64)))
FROM numbers(30) ORDER BY cityHash64(number) SETTINGS max_block_size = 7;

SET force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.v.:Int64 = 18;
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.v.:String = '19';
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.sparse = 'hit-20';
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE has(j.arr, 117);
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.z = 0.0;
SELECT count() FROM json_bf_ranges WHERE j.nz = 0.0;
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.v.:Int64 = 18 OR has(j.arr, 117);
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.z = 0.0 AND j.sparse = 'hit-20';
SELECT count() FROM json_bf_ranges WHERE hasAll(j.arr, [17, 117]);
SELECT count() FROM json_bf_ranges WHERE hasAny(j.arr, [117, 123]);
SELECT count() FROM json_bf_ranges WHERE j.v = 18.0 SETTINGS dynamic_throw_on_type_mismatch = 0;

OPTIMIZE TABLE json_bf_ranges FINAL;
SELECT arraySort(groupArray(id)) FROM json_bf_ranges WHERE j.v.:Int64 = 18 OR has(j.arr, 117);
SELECT count() FROM json_bf_ranges WHERE j.z = 0.0 AND j.nz = 0.0;
DROP TABLE json_bf_ranges;

CREATE TABLE json_bf_blocks
(
    j JSON(id UInt64, other UInt64, common String),
    INDEX idx j TYPE jsonbf_v1() GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 65, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO json_bf_blocks SELECT ('{"id":' || toString(number) || ',"other":' || toString(number + 1000) || ',"common":"same"}')::JSON(id UInt64, other UInt64, common String) FROM numbers(130);
SELECT count() FROM json_bf_blocks WHERE j.id = 64 AND j.common = 'same';
SELECT count() FROM json_bf_blocks WHERE j.id = 129;
SELECT count() FROM json_bf_blocks WHERE j.id = 130;
SELECT count() FROM json_bf_blocks WHERE j.id = 64 OR j.common = 'same';
SELECT count() FROM json_bf_blocks WHERE j.id = 64 AND j.other = 1064;
SELECT count() FROM json_bf_blocks WHERE j.id = 64 AND j.other = 1065;
SELECT count() FROM json_bf_blocks WHERE j.id = 129 OR j.other = 1000;
SELECT count() FROM json_bf_blocks WHERE (j.id = 64 OR j.other = 1000) AND j.common = 'same';
DROP TABLE json_bf_blocks;
