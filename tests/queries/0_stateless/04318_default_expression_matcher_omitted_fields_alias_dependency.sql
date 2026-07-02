SET asterisk_include_alias_columns = 1;

DROP TABLE IF EXISTS default_expression_matcher_omitted_fields_alias_dependency;

CREATE TABLE default_expression_matcher_omitted_fields_alias_dependency
(
    a UInt8 DEFAULT 1,
    x UInt8 ALIAS a + 1,
    b UInt8 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE default_expression_matcher_omitted_fields_alias_dependency
    MODIFY COLUMN b UInt8 DEFAULT toUInt8(COLUMNS('^x$'))
    SETTINGS mutations_sync = 2;

INSERT INTO default_expression_matcher_omitted_fields_alias_dependency
SETTINGS input_format_defaults_for_omitted_fields = 1
FORMAT JSONEachRow
{"a":5}
{}
;

SELECT a, b
FROM default_expression_matcher_omitted_fields_alias_dependency
ORDER BY a DESC;

DROP TABLE default_expression_matcher_omitted_fields_alias_dependency;
