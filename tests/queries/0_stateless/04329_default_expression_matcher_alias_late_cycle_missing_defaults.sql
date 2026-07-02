DROP TABLE IF EXISTS default_expression_matcher_alias_late_cycle_missing_direct;
DROP TABLE IF EXISTS default_expression_matcher_alias_late_cycle_missing_indirect;
DROP TABLE IF EXISTS default_expression_matcher_materialized_late_cycle_missing;

SET asterisk_include_alias_columns = 0;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE default_expression_matcher_materialized_late_cycle_missing
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_materialized_late_cycle_missing ADD COLUMN m String MATERIALIZED b;

SET asterisk_include_materialized_columns = 1;

INSERT INTO default_expression_matcher_materialized_late_cycle_missing (a) VALUES (1); -- { serverError CYCLIC_ALIASES }

SET asterisk_include_materialized_columns = 0;

CREATE TABLE default_expression_matcher_alias_late_cycle_missing_direct
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_alias_late_cycle_missing_direct ADD COLUMN x String ALIAS b;

SET asterisk_include_alias_columns = 1;

INSERT INTO default_expression_matcher_alias_late_cycle_missing_direct (a) VALUES (1); -- { serverError CYCLIC_ALIASES }

SET asterisk_include_alias_columns = 0;

CREATE TABLE default_expression_matcher_alias_late_cycle_missing_indirect
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_alias_late_cycle_missing_indirect ADD COLUMN y String ALIAS b;
ALTER TABLE default_expression_matcher_alias_late_cycle_missing_indirect ADD COLUMN x String ALIAS y;

SET asterisk_include_alias_columns = 1;

INSERT INTO default_expression_matcher_alias_late_cycle_missing_indirect (a) VALUES (1); -- { serverError CYCLIC_ALIASES }

DROP TABLE default_expression_matcher_alias_late_cycle_missing_indirect;
DROP TABLE default_expression_matcher_alias_late_cycle_missing_direct;
DROP TABLE default_expression_matcher_materialized_late_cycle_missing;
