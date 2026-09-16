DROP TABLE IF EXISTS default_expression_matcher_clear_column_preserves_schema;

CREATE TABLE default_expression_matcher_clear_column_preserves_schema
(
    a UInt8,
    b UInt8 DEFAULT plus(COLUMNS('^a$'), 1)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_clear_column_preserves_schema (a) VALUES (1);
ALTER TABLE default_expression_matcher_clear_column_preserves_schema CLEAR COLUMN a SETTINGS mutations_sync = 1;
SELECT b FROM default_expression_matcher_clear_column_preserves_schema;

DROP TABLE default_expression_matcher_clear_column_preserves_schema;
