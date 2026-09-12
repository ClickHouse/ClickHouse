DROP TABLE IF EXISTS default_expression_matcher_alter_revalidates_stored_defaults;

CREATE TABLE default_expression_matcher_alter_revalidates_stored_defaults
(
    a UInt8,
    b UInt8 DEFAULT plus(COLUMNS('^a'), 1)
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_alter_revalidates_stored_defaults ADD COLUMN c UInt8;
INSERT INTO default_expression_matcher_alter_revalidates_stored_defaults (a, c) VALUES (1, 9);
SELECT b FROM default_expression_matcher_alter_revalidates_stored_defaults;

ALTER TABLE default_expression_matcher_alter_revalidates_stored_defaults ADD COLUMN a2 UInt8; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE default_expression_matcher_alter_revalidates_stored_defaults;
