-- Tags: no-fasttest, no-ordinary-database

-- Tests vector search in ClickHouse, i.e. Usearch indexes. Both index types share similarities in implementation and usage,
-- therefore they are tested in a single file.

-- This file contains tests for the non-standard default granularity of vector search indexes.

SET allow_experimental_usearch_index = 1;

SELECT 'Test the default index granularity for vector search indexes (CREATE TABLE AND ALTER TABLE), should be 100 million for USearch';

SELECT '- Usearch';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx(vec) TYPE usearch) ENGINE=MergeTree ORDER BY id;
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

DROP TABLE tab;
CREATE TABLE tab (id Int32, vec Array(Float32)) ENGINE=MergeTree ORDER BY id;
ALTER TABLE tab ADD INDEX idx(vec) TYPE usearch;
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

DROP TABLE tab;
