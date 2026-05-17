DROP TABLE IF EXISTS index_matcher_alias_asterisk;
DROP TABLE IF EXISTS index_matcher_alias_nested_matcher;
DROP TABLE IF EXISTS index_matcher_alias_late_cycle;

SET asterisk_include_alias_columns = 1;

CREATE TABLE index_matcher_alias_asterisk
(
    a UInt8,
    b UInt8 ALIAS a + 1,
    INDEX idx(*) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO index_matcher_alias_asterisk (a) VALUES (1), (3);
SELECT count() FROM index_matcher_alias_asterisk WHERE b = 2 SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE index_matcher_alias_nested_matcher
(
    a UInt8,
    c UInt8 ALIAS a + 1,
    b UInt8 ALIAS greatest(COLUMNS('^c$')),
    INDEX idx(COLUMNS('^b$')) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO index_matcher_alias_nested_matcher (a) VALUES (1), (3);
SELECT b FROM index_matcher_alias_nested_matcher ORDER BY a;

SET asterisk_include_alias_columns = 0;

CREATE TABLE index_matcher_alias_late_cycle
(
    a UInt8,
    x String ALIAS toJSONString(tuple(*))
)
ENGINE = MergeTree
ORDER BY tuple();

SET asterisk_include_alias_columns = 1;
ALTER TABLE index_matcher_alias_late_cycle ADD INDEX idx(x) TYPE set(0) GRANULARITY 1; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_alias_columns = 0;

DROP TABLE index_matcher_alias_late_cycle;
DROP TABLE index_matcher_alias_nested_matcher;
DROP TABLE index_matcher_alias_asterisk;
