SET asterisk_include_alias_columns = 1;

DROP TABLE IF EXISTS default_expression_matcher_alias_ttl;
DROP TABLE IF EXISTS default_expression_matcher_alias_conversion;

CREATE TABLE default_expression_matcher_alias_ttl
(
    ts DateTime,
    a UInt8,
    x UInt8 ALIAS a + 1,
    b UInt8 DEFAULT 0
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO default_expression_matcher_alias_ttl (ts, a, b) VALUES (toDateTime('2000-01-01 00:00:00'), 1, 99);

SELECT b FROM default_expression_matcher_alias_ttl;

ALTER TABLE default_expression_matcher_alias_ttl
    MODIFY COLUMN b UInt8 DEFAULT toUInt8(COLUMNS('^x$')) TTL ts
    SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE default_expression_matcher_alias_ttl FINAL;

SELECT a, b FROM default_expression_matcher_alias_ttl;

DROP TABLE default_expression_matcher_alias_ttl;

CREATE TABLE default_expression_matcher_alias_conversion
(
    a UInt8,
    x UInt8 ALIAS a + 1,
    b Nullable(UInt8) DEFAULT 0
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_alias_conversion (a, b) VALUES (1, NULL);

ALTER TABLE default_expression_matcher_alias_conversion
    MODIFY COLUMN b Nullable(UInt8) DEFAULT toUInt8(COLUMNS('^x$'))
    SETTINGS mutations_sync = 2;

ALTER TABLE default_expression_matcher_alias_conversion
    MODIFY COLUMN b UInt8 DEFAULT toUInt8(COLUMNS('^x$'))
    SETTINGS mutations_sync = 2;

SELECT a, b FROM default_expression_matcher_alias_conversion;

DROP TABLE default_expression_matcher_alias_conversion;
