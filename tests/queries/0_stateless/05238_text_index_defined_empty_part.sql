-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- Empty V3 defined part: INSERT then ALTER DELETE WHERE 1 must stay readable.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS text_index_defined_empty_part;

CREATE TABLE text_index_defined_empty_part
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 8,
    index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    remove_empty_parts = 0;

INSERT INTO text_index_defined_empty_part VALUES
    (0, 'alpha'), (1, NULL), (2, ''), (3, 'alpha'),
    (4, 'alpha'), (5, NULL), (6, ''), (7, 'beta'),
    (8, NULL), (9, NULL), (10, ''), (11, 'alpha'),
    (12, 'beta'), (13, NULL), (14, ''), (15, 'alpha'),
    (16, 'alpha');

ALTER TABLE text_index_defined_empty_part DELETE WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'rows';
SELECT rows FROM system.parts
WHERE database = currentDatabase() AND table = 'text_index_defined_empty_part' AND active
ORDER BY name;

SELECT 'indexed';
SELECT count() FROM text_index_defined_empty_part;
SELECT count() FROM text_index_defined_empty_part WHERE isNull(value);
SELECT count() FROM text_index_defined_empty_part WHERE isNotNull(value);
SELECT count() FROM text_index_defined_empty_part WHERE hasToken(value, 'alpha');
SELECT count() FROM text_index_defined_empty_part WHERE NOT hasToken(value, 'alpha');

SELECT 'scan';
SELECT count() FROM text_index_defined_empty_part
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM text_index_defined_empty_part WHERE isNull(value)
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM text_index_defined_empty_part WHERE hasToken(value, 'alpha')
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM text_index_defined_empty_part WHERE NOT hasToken(value, 'alpha')
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE text_index_defined_empty_part;
