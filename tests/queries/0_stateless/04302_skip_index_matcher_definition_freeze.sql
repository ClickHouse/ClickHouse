DROP TABLE IF EXISTS skip_index_matcher_definition_freeze;
DROP TABLE IF EXISTS skip_index_matcher_definition_freeze_alter;
DROP TABLE IF EXISTS skip_index_matcher_definition_freeze_multi;

CREATE TABLE skip_index_matcher_definition_freeze
(
    a UInt64,
    INDEX idx(COLUMNS('^a')) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze';

ALTER TABLE skip_index_matcher_definition_freeze ADD COLUMN a2 UInt64;

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze';

DROP TABLE skip_index_matcher_definition_freeze;

CREATE TABLE skip_index_matcher_definition_freeze_alter
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE skip_index_matcher_definition_freeze_alter ADD INDEX idx(COLUMNS('^a')) TYPE minmax GRANULARITY 1;

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze_alter';

ALTER TABLE skip_index_matcher_definition_freeze_alter ADD COLUMN a2 UInt64;

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze_alter';

DROP TABLE skip_index_matcher_definition_freeze_alter;

CREATE TABLE skip_index_matcher_definition_freeze_multi
(
    a1 UInt64,
    a2 UInt64,
    INDEX idx(COLUMNS('^a')) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze_multi';

ALTER TABLE skip_index_matcher_definition_freeze_multi ADD COLUMN a3 UInt64;

SELECT extract(create_table_query, 'INDEX idx \\([^)]*\\) TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_matcher_definition_freeze_multi';

DROP TABLE skip_index_matcher_definition_freeze_multi;
