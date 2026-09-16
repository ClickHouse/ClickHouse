DROP TABLE IF EXISTS default_expression_matcher_alias_missing_defaults;

CREATE TABLE default_expression_matcher_alias_missing_defaults
(
    a String,
    x String ALIAS concat(a, '!'),
    b UInt64 DEFAULT length(COLUMNS('^x$'))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_alias_missing_defaults (a) VALUES ('ab');
SELECT b FROM default_expression_matcher_alias_missing_defaults;

DROP TABLE default_expression_matcher_alias_missing_defaults;
