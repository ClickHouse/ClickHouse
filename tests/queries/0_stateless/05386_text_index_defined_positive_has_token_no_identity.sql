-- Tags: no-random-merge-tree-settings

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS text_index_defined_positive_has_token;

CREATE TABLE text_index_defined_positive_has_token
(
    id UInt32,
    value Nullable(String),
    INDEX value_index value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_positive_has_token;

INSERT INTO text_index_defined_positive_has_token VALUES
    (0, 'alpha'), (1, 'beta'), (2, NULL), (3, ''),
    (4, 'alpha'), (5, NULL), (6, 'beta'), (7, 'alpha');

SELECT 'filter';
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') ORDER BY id;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') AND id >= 0 ORDER BY id;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') OR id = 2 ORDER BY id;

SELECT 'null_sensitive';
SELECT id FROM text_index_defined_positive_has_token WHERE NOT hasToken(value, 'alpha') ORDER BY id;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') = 0 ORDER BY id;

SELECT 'scan';
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') AND id >= 0 ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') OR id = 2 ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_positive_has_token WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_positive_has_token WHERE hasToken(value, 'alpha') = 0 ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE text_index_defined_positive_has_token;
