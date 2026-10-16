-- A `bloom_filter` index on a typed JSON path is used when the path is cast to or from `Nullable`, which does not
-- change a non-NULL value.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS entity_models;

CREATE TABLE entity_models
(
    RawDocument String,
    Document JSON(Name String, TypeName Nullable(String)) MATERIALIZED RawDocument,
    Id String,
    INDEX index_document_name Document.Name TYPE bloom_filter(0.025) GRANULARITY 1,
    INDEX index_document_typename Document.TypeName TYPE bloom_filter(0.025) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY Id
SETTINGS cache_populated_by_fetch = 1, merge_max_block_size = 128, write_marks_for_substreams_in_compact_parts = 1,
    merge_max_dynamic_subcolumns_in_compact_part = 0, merge_max_dynamic_subcolumns_in_wide_part = 0,
    min_bytes_for_wide_part = 134217728, object_shared_data_buckets_for_wide_part = 8,
    allow_part_offset_column_in_projections = 1, index_granularity = 1;

INSERT INTO entity_models (Id, RawDocument) VALUES
('1', '{"Name":"a","TypeName":"User","Entity":{"DisplayName":"disp1","ScopeIds":["s1"]},"TenantId":"t1","SystemDeleted":false}'),
('2', '{"Name":"b","TypeName":"Group","Entity":{"DisplayName":"disp2","ScopeIds":["s2"]},"TenantId":"t1","SystemDeleted":false}');

SELECT '-- String path, no cast';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM entity_models WHERE has(['a'], Document.Name)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT Id FROM entity_models WHERE has(['a'], Document.Name);

SELECT '-- String path, cast to Nullable(String)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM entity_models WHERE has(['a'], Document.Name::Nullable(String))) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT Id FROM entity_models WHERE has(['a'], Document.Name::Nullable(String));

SELECT '-- Nullable(String) path, no cast';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM entity_models WHERE has(['User'], Document.TypeName)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT Id FROM entity_models WHERE has(['User'], Document.TypeName);

SELECT '-- Nullable(String) path, cast to String';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM entity_models WHERE has(['User'], Document.TypeName::String)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT Id FROM entity_models WHERE has(['User'], Document.TypeName::String);

DROP TABLE entity_models;
