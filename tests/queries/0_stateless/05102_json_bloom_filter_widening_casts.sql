DROP TABLE IF EXISTS json_bf_widening;
CREATE TABLE json_bf_widening (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;

-- Preserve narrow runtime types so widening casts exercise their original value tokens.
INSERT INTO json_bf_widening
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt8(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt32(-42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt32(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt8(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt32(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt32(4294967295) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat32(42.5) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat32(0.1) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(9)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(42.5) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(10)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(256) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(11)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt8(2) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(12)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toDecimal64('42.9', 1) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(13)), unhex('010178'), formatRowNoNewline('RowBinary', CAST('42' AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;

SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = 42 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = 42 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = '42' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = '42' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = '2147483648' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = '2147483648' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt64) = 42 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt64) = 42 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt64) = '4294967296' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt64) = '4294967296' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = 42.5 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = 42.5 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = '0.10000000149011612' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = '0.10000000149011612' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = '0.1' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = '0.1' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = 42 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = 42 SETTINGS use_skip_indexes = 0;
-- Decimal comparisons after an integer-to-float cast can round the constant.
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = toDecimal128('42.000000000000000001', 18) SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Float64) = toDecimal128('42.000000000000000001', 18) SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int8) = 0 SETTINGS use_skip_indexes = 1;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int8) = 0 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt8) = 0 SETTINGS use_skip_indexes = 1;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS UInt8) = 0 SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Bool) = 1 SETTINGS use_skip_indexes = 1; -- { serverError CANNOT_PARSE_BOOL }
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Bool) = 1 SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_PARSE_BOOL }
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = 42.5 SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_widening WHERE CAST(j.x AS Int64) = 42.5 SETTINGS use_skip_indexes = 0;

-- Failed comparisons still execute and report their conversion error.
SELECT count() FROM json_bf_widening WHERE CAST(j.x AS Int64) = 'invalid' SETTINGS force_data_skipping_indices = 'bf'; -- { serverError CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT, TYPE_MISMATCH }
SELECT count() FROM json_bf_widening WHERE CAST(j.x AS Int64) = 'invalid' SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT, TYPE_MISMATCH }

-- All values fit the widening cast, but the constant is outside the original type's range.
TRUNCATE TABLE json_bf_widening;
INSERT INTO json_bf_widening
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt32(42) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt8(42) AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_widening WHERE CAST(j.x AS UInt64) = '4294967296'
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/2';
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_widening WHERE CAST(j.x AS Int64) = 123456789
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/2';
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_widening WHERE CAST(j.x AS Float64) = 123456789
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/2';
DROP TABLE json_bf_widening;
