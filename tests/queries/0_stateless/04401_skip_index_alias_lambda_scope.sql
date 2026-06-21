DROP TABLE IF EXISTS skip_index_alias_lambda_scope;

CREATE TABLE skip_index_alias_lambda_scope
(
    a UInt8,
    x UInt8 ALIAS a + 1,
    lim UInt8 ALIAS a + 2,
    arr Array(UInt8),
    INDEX idx arrayCount(x -> x < lim, arr) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO skip_index_alias_lambda_scope (a, arr) VALUES (2, [1, 2, 3, 4]);

SELECT extract(create_table_query, 'INDEX idx .+ TYPE minmax GRANULARITY 1')
FROM system.tables
WHERE database = currentDatabase() AND name = 'skip_index_alias_lambda_scope';

SELECT count()
FROM skip_index_alias_lambda_scope
WHERE arrayCount(x -> x < lim, arr) = 3;

DROP TABLE skip_index_alias_lambda_scope;
