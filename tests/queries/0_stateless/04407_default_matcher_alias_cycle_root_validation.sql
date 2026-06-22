DROP TABLE IF EXISTS default_matcher_alias_cycle_root_validation;
DROP TABLE IF EXISTS default_matcher_alias_cycle_root_validation_merge;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE default_matcher_alias_cycle_root_validation
(
    a UInt8,
    x String ALIAS toJSONString(tuple(* EXCEPT x)),
    m String MATERIALIZED x
)
ENGINE = Memory;

INSERT INTO default_matcher_alias_cycle_root_validation (a) VALUES (1);

SET asterisk_include_materialized_columns = 1;

SELECT x FROM default_matcher_alias_cycle_root_validation
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_materialized_columns = 0;

CREATE TABLE default_matcher_alias_cycle_root_validation_merge
(
    a UInt8,
    x String ALIAS toJSONString(tuple(* EXCEPT x)),
    m String MATERIALIZED x
)
ENGINE = Memory;

INSERT INTO default_matcher_alias_cycle_root_validation_merge (a) VALUES (1);

SET asterisk_include_materialized_columns = 1;

SELECT x FROM merge(currentDatabase(), '^default_matcher_alias_cycle_root_validation_merge$')
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1; -- { serverError CYCLIC_ALIASES }

DROP TABLE default_matcher_alias_cycle_root_validation_merge;
DROP TABLE default_matcher_alias_cycle_root_validation;
