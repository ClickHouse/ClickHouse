SET enable_named_columns_in_function_tuple = 1;

DROP TABLE IF EXISTS index_alias_default_expr_matchers_asterisk;
DROP TABLE IF EXISTS index_alias_default_expr_matchers_columns;

CREATE TABLE index_alias_default_expr_matchers_asterisk
(
    a UInt8,
    c UInt8,
    b UInt8 ALIAS greatest(*),
    INDEX idx(b) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO index_alias_default_expr_matchers_asterisk (a, c) VALUES (1, 3), (5, 2);
SELECT b FROM index_alias_default_expr_matchers_asterisk ORDER BY a;
SELECT count() FROM index_alias_default_expr_matchers_asterisk WHERE b = 3 SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE index_alias_default_expr_matchers_columns
(
    a UInt8,
    c UInt8,
    d UInt8,
    b UInt8 ALIAS greatest(COLUMNS('^(a|c)$')),
    INDEX idx(b) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO index_alias_default_expr_matchers_columns (a, c, d) VALUES (1, 3, 9), (5, 2, 8);
SELECT b FROM index_alias_default_expr_matchers_columns ORDER BY a;
SELECT count() FROM index_alias_default_expr_matchers_columns WHERE b = 3 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE index_alias_default_expr_matchers_columns;
DROP TABLE index_alias_default_expr_matchers_asterisk;
