DROP TABLE IF EXISTS alias_matcher_late_cycle_new_analyzer;
DROP TABLE IF EXISTS alias_matcher_late_cycle_new_analyzer_indirect;
DROP TABLE IF EXISTS alias_matcher_late_cycle_old_analyzer;
DROP TABLE IF EXISTS alias_matcher_late_cycle_old_analyzer_indirect;

SET allow_experimental_analyzer = 1;
SET asterisk_include_alias_columns = 0;

CREATE TABLE alias_matcher_late_cycle_new_analyzer
(
    a UInt8,
    x String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_late_cycle_new_analyzer; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_alias_columns = 0;
CREATE TABLE alias_matcher_late_cycle_new_analyzer_indirect
(
    a UInt8,
    x String ALIAS toJSONString(tuple(* EXCEPT x)),
    y String ALIAS toJSONString(tuple(* EXCEPT y))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_late_cycle_new_analyzer_indirect; -- { serverError CYCLIC_ALIASES }

SET allow_experimental_analyzer = 0;
SET asterisk_include_alias_columns = 0;

CREATE TABLE alias_matcher_late_cycle_old_analyzer
(
    a UInt8,
    x String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_late_cycle_old_analyzer SETTINGS optimize_respect_aliases = 1; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_alias_columns = 0;
CREATE TABLE alias_matcher_late_cycle_old_analyzer_indirect
(
    a UInt8,
    x String ALIAS toJSONString(tuple(* EXCEPT x)),
    y String ALIAS toJSONString(tuple(* EXCEPT y))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
SELECT x FROM alias_matcher_late_cycle_old_analyzer_indirect SETTINGS optimize_respect_aliases = 1; -- { serverError CYCLIC_ALIASES }

DROP TABLE alias_matcher_late_cycle_old_analyzer_indirect;
DROP TABLE alias_matcher_late_cycle_old_analyzer;
DROP TABLE alias_matcher_late_cycle_new_analyzer_indirect;
DROP TABLE alias_matcher_late_cycle_new_analyzer;
