DROP TABLE IF EXISTS alter_default_existing_alias_cycle;
DROP TABLE IF EXISTS alter_default_matcher_drop_dependency_legacy;
DROP TABLE IF EXISTS alter_default_matcher_drop_dependency_analyzer;

CREATE TABLE alter_default_existing_alias_cycle
(
    y UInt8,
    x UInt8 ALIAS y + 1
)
ENGINE = Memory;

ALTER TABLE alter_default_existing_alias_cycle MODIFY COLUMN y UInt8 DEFAULT x + 1; -- { serverError CYCLIC_ALIASES }

CREATE TABLE alter_default_matcher_drop_dependency_legacy
(
    a UInt8,
    b UInt8 DEFAULT greatest(COLUMNS('^a$'))
)
ENGINE = Memory;

ALTER TABLE alter_default_matcher_drop_dependency_legacy DROP COLUMN a SETTINGS allow_experimental_analyzer = 0; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE alter_default_matcher_drop_dependency_analyzer
(
    a UInt8,
    b UInt8 DEFAULT greatest(COLUMNS('^a$'))
)
ENGINE = Memory;

ALTER TABLE alter_default_matcher_drop_dependency_analyzer DROP COLUMN a SETTINGS allow_experimental_analyzer = 1; -- { serverError ILLEGAL_COLUMN }

DROP TABLE alter_default_matcher_drop_dependency_analyzer;
DROP TABLE alter_default_matcher_drop_dependency_legacy;
DROP TABLE alter_default_existing_alias_cycle;
