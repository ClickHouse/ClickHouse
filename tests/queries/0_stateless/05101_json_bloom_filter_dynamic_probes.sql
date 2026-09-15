DROP TABLE IF EXISTS json_bf_dynamic_probes;
CREATE TABLE json_bf_dynamic_probes (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;

-- `RowBinary` preserves the runtime types; casting a map to `JSON` serializes and infers them again.
INSERT INTO json_bf_dynamic_probes
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', (toInt64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', (toUInt64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', (toFloat64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', (toDecimal64('42.00', 2))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST('42' AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', (toInt32(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', (toFloat32(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', (toDecimal128('42.000', 3))::Dynamic))) SETTINGS input_format_binary_read_json_as_string = 0;

SELECT id, dynamicType(j.x) FROM json_bf_dynamic_probes ORDER BY id;
SELECT 'string', arraySort(groupArray(id)) FROM json_bf_dynamic_probes WHERE j.x = '42' SETTINGS force_data_skipping_indices = 'bf';
SELECT 'string without index', arraySort(groupArray(id)) FROM json_bf_dynamic_probes WHERE j.x = '42' SETTINGS use_skip_indexes = 0;
SELECT 'constant left', arraySort(groupArray(id)) FROM json_bf_dynamic_probes WHERE '42' = j.x SETTINGS force_data_skipping_indices = 'bf';
SELECT 'typed decimal', arraySort(groupArray(id)) FROM json_bf_dynamic_probes WHERE j.x.:`Decimal(18, 2)` = toDecimal64('42.00', 2) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'cast', arraySort(groupArray(id)) FROM json_bf_dynamic_probes WHERE CAST(j.x AS Int64) = '42' SETTINGS force_data_skipping_indices = 'bf';
SELECT 'absent', count() FROM json_bf_dynamic_probes WHERE j.x = '123456789' SETTINGS force_data_skipping_indices = 'bf';
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM json_bf_dynamic_probes WHERE j.x = '123456789'
    SETTINGS force_data_skipping_indices = 'bf', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/8';

-- A failed string conversion must keep the exception when the affected runtime type is present.
SELECT count() FROM json_bf_dynamic_probes WHERE j.x = 'not-a-number' SETTINGS force_data_skipping_indices = 'bf'; -- { serverError TYPE_MISMATCH, CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT }
SELECT count() FROM json_bf_dynamic_probes WHERE j.x = 'not-a-number' SETTINGS use_skip_indexes = 0; -- { serverError TYPE_MISMATCH, CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT }
SELECT count() FROM json_bf_dynamic_probes WHERE j.x = '-42' SETTINGS force_data_skipping_indices = 'bf'; -- { serverError TYPE_MISMATCH, CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT }
SELECT count() FROM json_bf_dynamic_probes WHERE j.x = '-42' SETTINGS use_skip_indexes = 0; -- { serverError TYPE_MISMATCH, CANNOT_PARSE_NUMBER, CANNOT_PARSE_TEXT }

-- Keep the same probes correct when every path and runtime type is stored in shared data.
CREATE TABLE json_bf_dynamic_shared (id UInt64, j JSON(max_dynamic_paths = 0, max_dynamic_types = 0), INDEX bf j TYPE jsonbf_v1() GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO json_bf_dynamic_shared
SELECT * FROM format(RowBinary, 'id UInt64, j JSON(max_dynamic_paths = 0, max_dynamic_types = 0)', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', (toInt64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', (toUInt64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', (toFloat64(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', (toDecimal64('42.00', 2))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST('42' AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', (toInt32(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', (toFloat32(42))::Dynamic),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', (toDecimal128('42.000', 3))::Dynamic))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT 'shared types', arraySort(groupArray(dynamicType(j.x))) FROM json_bf_dynamic_shared;
SELECT 'shared string', arraySort(groupArray(id)) FROM json_bf_dynamic_shared WHERE j.x = '42' SETTINGS force_data_skipping_indices = 'bf', use_skip_indexes_on_data_read = 1, max_threads = 4;
SELECT 'shared string without index', arraySort(groupArray(id)) FROM json_bf_dynamic_shared WHERE j.x = '42' SETTINGS use_skip_indexes = 0;
SELECT 'shared absent', count() FROM json_bf_dynamic_shared WHERE j.x = '123456789' SETTINGS force_data_skipping_indices = 'bf', use_skip_indexes_on_data_read = 1;
DROP TABLE json_bf_dynamic_shared;
DROP TABLE json_bf_dynamic_probes;

-- Numeric equality must stay exact at integer/float boundaries and for signed zero.
CREATE TABLE json_bf_numeric_boundaries (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;
INSERT INTO json_bf_numeric_boundaries
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(9007199254740993) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(3)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(9007199254740992) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(4)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(18446744073709551615) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(5)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(18446744073709551616.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(6)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(-9223372036854775808) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(7)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(-9223372036854775808.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(8)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64(-0.) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(9)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toUInt64(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(10)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toInt64(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(11)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat32(0) AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(12)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('nan') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(13)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('inf') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(14)), unhex('010178'), formatRowNoNewline('RowBinary', CAST(toFloat64('-inf') AS Dynamic)))) SETTINGS input_format_binary_read_json_as_string = 0;
SELECT 'exact integer', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = 9007199254740993 SETTINGS force_data_skipping_indices = 'bf';
SELECT 'exact integer without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = 9007199254740993 SETTINGS use_skip_indexes = 0;
SELECT 'exact float', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64(9007199254740992) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'exact float without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64(9007199254740992) SETTINGS use_skip_indexes = 0;
SELECT 'uint64 max', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toUInt64(18446744073709551615) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'uint64 max without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toUInt64(18446744073709551615) SETTINGS use_skip_indexes = 0;
SELECT 'float above uint64', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64(18446744073709551616.) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'float above uint64 without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64(18446744073709551616.) SETTINGS use_skip_indexes = 0;
SELECT 'int64 min', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toInt64(-9223372036854775808) SETTINGS force_data_skipping_indices = 'bf';
SELECT 'int64 min without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toInt64(-9223372036854775808) SETTINGS use_skip_indexes = 0;
SELECT 'zero', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = 0 SETTINGS force_data_skipping_indices = 'bf';
SELECT 'zero without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = 0 SETTINGS use_skip_indexes = 0;
SELECT 'nan', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('nan') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'nan without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('nan') SETTINGS use_skip_indexes = 0;
SELECT 'infinity', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('inf') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'infinity without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('inf') SETTINGS use_skip_indexes = 0;
SELECT 'negative infinity', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('-inf') SETTINGS force_data_skipping_indices = 'bf';
SELECT 'negative infinity without index', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x = toFloat64('-inf') SETTINGS use_skip_indexes = 0;
SELECT 'typed int64 zero', arraySort(groupArray(id)) FROM json_bf_numeric_boundaries WHERE j.x.:Int64 = 0 SETTINGS force_data_skipping_indices = 'bf';
DROP TABLE json_bf_numeric_boundaries;
