DROP TABLE IF EXISTS skip_index_alias_lambda_normalization;

CREATE TABLE skip_index_alias_lambda_normalization
(
    a UInt8,
    arr Array(UInt8),
    x UInt8 ALIAS a + 1,
    lim UInt8 ALIAS x + 1,
    INDEX idx arrayCount(x -> x < lim, arr) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT extract(create_table_query, 'INDEX idx .* TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_alias_lambda_normalization';

DROP TABLE skip_index_alias_lambda_normalization;
