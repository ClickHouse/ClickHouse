-- Matcher expansion in stored expressions is settings-independent: an ALIAS whose
-- expression uses `*` reads the same value regardless of `asterisk_include_alias_columns`,
-- and no settings-driven late cycle can form.
DROP TABLE IF EXISTS alias_matcher_read_new_analyzer;

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

DROP TABLE alias_matcher_read_new_analyzer;
