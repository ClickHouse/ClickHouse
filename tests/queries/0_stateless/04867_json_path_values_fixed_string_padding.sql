-- FixedString values compare zero-padded ('a' = toFixedString('a', 3)), but `jsonPathValues`
-- tokens store exact bytes. The text index must not prune rows for predicates with
-- FixedString needles on String or Dynamic paths.

SET enable_json_type = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_padding;
CREATE TABLE json_pv_fixed_string_padding
(
    id UInt64,
    json JSON,
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_padding;
INSERT INTO json_pv_fixed_string_padding VALUES (1, '{"k":"a"}'), (2, '{"k":"ab"}'), (3, '{"k":"b"}');

SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String IN (SELECT toFixedString('a', 3));
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String = toFixedString('a', 3);
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k = toFixedString('a', 3);
SELECT count() FROM json_pv_fixed_string_padding WHERE has(['a', 'b']::Array(FixedString(3)), json.k.:String);

-- The same predicates must return identical results without the index.
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String IN (SELECT toFixedString('a', 3)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String = toFixedString('a', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k = toFixedString('a', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE has(['a', 'b']::Array(FixedString(3)), json.k.:String) SETTINGS use_skip_indexes = 0;

DROP TABLE json_pv_fixed_string_padding;
