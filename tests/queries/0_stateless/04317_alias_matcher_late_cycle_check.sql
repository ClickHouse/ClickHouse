-- Matcher expansion in stored expressions is settings-independent: an ALIAS whose
-- expression uses `*` reads the same value regardless of `asterisk_include_alias_columns`,
-- under both analyzers, and no settings-driven late cycle can form.
DROP TABLE IF EXISTS alias_matcher_read_new_analyzer;
DROP TABLE IF EXISTS alias_matcher_read_old_analyzer;

SET allow_experimental_analyzer = 1;
SET asterisk_include_alias_columns = 0;

CREATE TABLE alias_matcher_read_new_analyzer
(
    a UInt8,
    x String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory;

INSERT INTO alias_matcher_read_new_analyzer (a) VALUES (1);

SELECT x FROM alias_matcher_read_new_analyzer;
SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_read_new_analyzer;

SET allow_experimental_analyzer = 0;
SET asterisk_include_alias_columns = 0;

CREATE TABLE alias_matcher_read_old_analyzer
(
    a UInt8,
    x String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory;

INSERT INTO alias_matcher_read_old_analyzer (a) VALUES (1);

SELECT x FROM alias_matcher_read_old_analyzer SETTINGS optimize_respect_aliases = 1;
SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_read_old_analyzer SETTINGS optimize_respect_aliases = 1;

DROP TABLE alias_matcher_read_old_analyzer;
DROP TABLE alias_matcher_read_new_analyzer;
