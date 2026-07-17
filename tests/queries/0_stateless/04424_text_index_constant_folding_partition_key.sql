-- Regression test: a text index whose column is also the partition key must not abort
-- with LOGICAL_ERROR "Query builder not found for text search query with function 'like'"
-- when use_constant_folding_in_index_analysis is enabled.
--
-- Direct-read virtual columns are registered against the unsubstituted condition, but the
-- granule analyzer was built from the per-partition constant-folded condition, whose search
-- queries hash differently. The reader then failed to find the query builder. Text indexes
-- now keep constant folding off so the granule and the reader use the same query set.

SET allow_experimental_full_text_index = 1;
SET use_constant_folding_in_index_analysis = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s)) GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY s ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO tab (s) VALUES ('Hello, world!');
INSERT INTO tab (s) VALUES ('ClickHouse is the fastest OLAP database');

SELECT s FROM tab WHERE s LIKE '%the fastest OLAP database%';
SELECT count() FROM tab WHERE hasToken(lower(s), 'clickhouse');

DROP TABLE tab;
