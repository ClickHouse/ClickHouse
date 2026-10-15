-- Tags: no-parallel-replicas, no-random-merge-tree-settings

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS text_index_defined_merge;
DROP TABLE IF EXISTS text_index_defined_merge_downgrade;
DROP TABLE IF EXISTS text_index_defined_materialize;

CREATE TABLE text_index_defined_merge
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO text_index_defined_merge VALUES (0, 'alpha'), (1, NULL), (2, '');
INSERT INTO text_index_defined_merge VALUES (3, 'alpha beta'), (4, NULL), (5, 'beta');
INSERT INTO text_index_defined_merge VALUES (6, 'alpha'), (7, NULL), (8, 'beta');

OPTIMIZE TABLE text_index_defined_merge FINAL SETTINGS mutations_sync = 2;

SELECT 'indexed';
SELECT id, value, hasToken(value, 'alpha'), isNull(value), isNotNull(value)
FROM text_index_defined_merge ORDER BY id;
SELECT id FROM text_index_defined_merge WHERE hasToken(value, 'beta') ORDER BY id;
SELECT id FROM text_index_defined_merge WHERE NOT hasToken(value, 'beta') ORDER BY id;

SELECT 'scan';
SELECT id, value, hasToken(value, 'alpha'), isNull(value), isNotNull(value)
FROM text_index_defined_merge ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_merge WHERE hasToken(value, 'beta') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT id FROM text_index_defined_merge WHERE NOT hasToken(value, 'beta') ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

CREATE TABLE text_index_defined_merge_downgrade
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO text_index_defined_merge_downgrade VALUES (0, 'alpha'), (1, NULL);
ALTER TABLE text_index_defined_merge_downgrade MODIFY SETTING text_index_serialization_version = 'v0_initial';
INSERT INTO text_index_defined_merge_downgrade VALUES (2, 'beta'), (3, NULL), (4, 'alpha');

OPTIMIZE TABLE text_index_defined_merge_downgrade FINAL SETTINGS mutations_sync = 2;

SELECT id, value, hasToken(value, 'alpha'), isNull(value), isNotNull(value)
FROM text_index_defined_merge_downgrade ORDER BY id;
SELECT id FROM text_index_defined_merge_downgrade WHERE NOT hasToken(value, 'alpha') ORDER BY id;

CREATE TABLE text_index_defined_materialize
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO text_index_defined_materialize VALUES (0, 'alpha'), (1, NULL), (2, 'beta');
ALTER TABLE text_index_defined_materialize UPDATE value = if(id = 1, 'gamma', value) WHERE id IN (1, 2) SETTINGS mutations_sync = 2;

SELECT id, value, hasToken(value, 'gamma'), isNull(value), isNotNull(value)
FROM text_index_defined_materialize ORDER BY id;
SELECT id FROM text_index_defined_materialize WHERE NOT hasToken(value, 'gamma') ORDER BY id;

DROP TABLE text_index_defined_merge;
DROP TABLE text_index_defined_merge_downgrade;
DROP TABLE text_index_defined_materialize;
