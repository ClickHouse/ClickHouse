-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS mv_indexes;
DROP TABLE IF EXISTS mv_no_indexes;
DROP TABLE IF EXISTS mv_projections;
DROP TABLE IF EXISTS mv_primary_key;
DROP TABLE IF EXISTS mv_primary_key_from_column;

CREATE TABLE data
(
    key String,
)
ENGINE = MergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW mv_indexes
(
    key String,
    INDEX idx key TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
AS SELECT * FROM data;

CREATE MATERIALIZED VIEW mv_no_indexes
(
    key String,
    INDEX idx key TYPE bloom_filter GRANULARITY 1
)
ENGINE = Null
AS SELECT * FROM data;

CREATE MATERIALIZED VIEW mv_projections
(
    key String,
    projection p (SELECT uniqCombined(key))
)
ENGINE = MergeTree
ORDER BY key
AS SELECT * FROM data;

CREATE MATERIALIZED VIEW mv_primary_key
(
    key String,
    PRIMARY KEY key
)
ENGINE = MergeTree
AS SELECT * FROM data;

CREATE MATERIALIZED VIEW mv_primary_key_from_column
(
    key String PRIMARY KEY
)
ENGINE = MergeTree
AS SELECT * FROM data;

SELECT replaceRegexpOne(create_table_query, 'CREATE TABLE [^ ]*', 'CREATE TABLE x') FROM system.tables WHERE database = currentDatabase() and table LIKE '.inner%' ORDER BY 1 FORMAT LineAsString;
