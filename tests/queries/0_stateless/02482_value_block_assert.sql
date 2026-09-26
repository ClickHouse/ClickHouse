SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE range_key_dictionary_source_table__fuzz_323
(
    `key` UInt256,
    `start_date` Int8,
    `end_date` LowCardinality(UInt256),
    `value` Tuple(UInt8, Array(DateTime), Decimal(9, 1), Array(Int16), Array(UInt8)),
    `value_nullable` UUID
)
ENGINE = TinyLog;
INSERT INTO range_key_dictionary_source_table__fuzz_323 FORMAT Values
(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First'); -- { error CANNOT_PARSE_INPUT_ASSERTION_FAILED }


CREATE TABLE complex_key_dictionary_source_table__fuzz_267
(
    `id` Decimal(38, 30),
    `id_key` Array(UUID),
    `value` Array(Nullable(DateTime64(3))),
    `value_nullable` Nullable(UUID)
)
ENGINE = TinyLog;
INSERT INTO complex_key_dictionary_source_table__fuzz_267 FORMAT Values
(1, 'key', 'First', 'First'); -- { error CANNOT_READ_ARRAY_FROM_TEXT }
