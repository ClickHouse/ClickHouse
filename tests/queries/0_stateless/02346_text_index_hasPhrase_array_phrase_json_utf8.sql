-- Tags: no-fasttest
-- Tag no-fasttest: lowerUTF8 is only available in builds with ICU.

-- `lowerUTF8` is context-sensitive (final sigma), so array needles on a JSON path covered by a `JSONAllValues` text index
-- with this preprocessor must not be pruned with preprocessed needles.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET query_plan_direct_read_from_text_index = 1;

CREATE TABLE tab_utf8_case_mapping
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lowerUTF8(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_utf8_case_mapping VALUES (1, '{"key": "ΟΔΟΣ''Α"}'), (2, '{"key": "dog"}');

SELECT 'UTF-8 case mapping preprocessor';
SELECT groupArray(id) FROM (SELECT id FROM tab_utf8_case_mapping WHERE hasAnyTokens(data.key, ['ΟΔΟΣ']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_utf8_case_mapping WHERE hasAnyTokens(data.key, ['ΟΔΟΣ']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_utf8_case_mapping WHERE hasPhrase(data.key, ['ΟΔΟΣ']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_utf8_case_mapping WHERE hasPhrase(data.key, ['ΟΔΟΣ']) ORDER BY id) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_utf8_case_mapping;
