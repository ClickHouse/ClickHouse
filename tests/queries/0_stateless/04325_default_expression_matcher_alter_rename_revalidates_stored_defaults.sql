DROP TABLE IF EXISTS default_expression_matcher_alter_rename_revalidates_stored_defaults;
DROP TABLE IF EXISTS default_expression_matcher_alter_rename_revalidates_identifier_default;

CREATE TABLE default_expression_matcher_alter_rename_revalidates_identifier_default
(
    a UInt8,
    b UInt8 DEFAULT plus(a, 1)
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_alter_rename_revalidates_identifier_default RENAME COLUMN a TO c;
INSERT INTO default_expression_matcher_alter_rename_revalidates_identifier_default (c) VALUES (1);
SELECT b FROM default_expression_matcher_alter_rename_revalidates_identifier_default;

CREATE TABLE default_expression_matcher_alter_rename_revalidates_stored_defaults
(
    a UInt8,
    b UInt8 DEFAULT plus(COLUMNS('^a$'), 1)
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_alter_rename_revalidates_stored_defaults RENAME COLUMN a TO c; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE default_expression_matcher_alter_rename_revalidates_stored_defaults;
DROP TABLE default_expression_matcher_alter_rename_revalidates_identifier_default;
