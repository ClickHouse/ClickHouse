DROP TABLE IF EXISTS default_expression_matcher_ttl;
DROP TABLE IF EXISTS default_expression_matcher_conversion;

CREATE TABLE default_expression_matcher_ttl
(
    ts DateTime,
    a UInt8,
    c UInt8,
    b UInt8 DEFAULT plus(COLUMNS('^[ac]$'))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO default_expression_matcher_ttl (ts, a, c, b) VALUES (toDateTime('2000-01-01 00:00:00'), 1, 3, 99);

SELECT b FROM default_expression_matcher_ttl;

ALTER TABLE default_expression_matcher_ttl
    MODIFY COLUMN b UInt8 DEFAULT plus(COLUMNS('^[ac]$')) TTL ts
    SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE default_expression_matcher_ttl FINAL;

SELECT a, c, b FROM default_expression_matcher_ttl;

DROP TABLE default_expression_matcher_ttl;

CREATE TABLE default_expression_matcher_conversion
(
    a UInt8,
    c UInt8,
    b Nullable(UInt8) DEFAULT plus(COLUMNS('^[ac]$'))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_conversion (a, c, b) VALUES (1, 3, NULL);

ALTER TABLE default_expression_matcher_conversion
    MODIFY COLUMN b UInt8 DEFAULT plus(COLUMNS('^[ac]$'))
    SETTINGS mutations_sync = 2;

SELECT a, c, b FROM default_expression_matcher_conversion;

DROP TABLE default_expression_matcher_conversion;
