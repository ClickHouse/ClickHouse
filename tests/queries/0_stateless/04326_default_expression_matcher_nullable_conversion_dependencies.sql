DROP TABLE IF EXISTS default_expression_matcher_nullable_conversion_dependencies;

CREATE TABLE default_expression_matcher_nullable_conversion_dependencies
(
    a UInt8,
    c UInt8,
    b Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_nullable_conversion_dependencies VALUES (1, 2, NULL), (3, 4, 9);

SYSTEM STOP MERGES default_expression_matcher_nullable_conversion_dependencies;

ALTER TABLE default_expression_matcher_nullable_conversion_dependencies
    MODIFY COLUMN b UInt8 DEFAULT plus(COLUMNS('^[ac]$'))
    SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT b FROM default_expression_matcher_nullable_conversion_dependencies ORDER BY a;

SYSTEM START MERGES default_expression_matcher_nullable_conversion_dependencies;

DROP TABLE default_expression_matcher_nullable_conversion_dependencies;
