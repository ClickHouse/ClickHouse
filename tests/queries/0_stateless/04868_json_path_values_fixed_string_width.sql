SET enable_json_type = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_width;
CREATE TABLE json_pv_fixed_string_width
(
    id UInt64,
    json JSON(k FixedString(3)),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_width;
INSERT INTO json_pv_fixed_string_width VALUES
    (1, '{"k":"a"}'),
    (2, '{"k":"b"}'),
    (3, '{"k":"c"}');

-- Equal-width `FixedString` values have identical padding, so the index remains usable.
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 3))
SETTINGS force_data_skipping_indices = 'idx';

-- Different widths compare equal after zero-padding, but their token bytes differ. The index must
-- decline this predicate instead of pruning the matching row.
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 4));
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 4))
SETTINGS use_skip_indexes = 0;

DROP TABLE json_pv_fixed_string_width;
