-- https://github.com/ClickHouse/ClickHouse/issues/52019
DROP TABLE IF EXISTS tab;
SET allow_experimental_inverted_index=1;
CREATE TABLE tab (`k` UInt64, `s` Map(String, String), INDEX af mapKeys(s) TYPE inverted(2) GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO tab (k) VALUES (0);
SELECT * FROM tab PREWHERE (s[NULL]) = 'Click a03' SETTINGS allow_experimental_analyzer=1; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT * FROM tab PREWHERE (s[1]) = 'Click a03' SETTINGS allow_experimental_analyzer=1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM tab PREWHERE (s['foo']) = 'Click a03' SETTINGS allow_experimental_analyzer=1;
DROP TABLE tab;
