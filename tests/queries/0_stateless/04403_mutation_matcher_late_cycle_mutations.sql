DROP TABLE IF EXISTS mutation_matcher_late_cycle_materialize;
DROP TABLE IF EXISTS mutation_matcher_late_cycle_clear;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE mutation_matcher_late_cycle_materialize
(
    a UInt8,
    m String MATERIALIZED toJSONString(tuple(*))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_matcher_late_cycle_materialize (a) VALUES (1);

SET asterisk_include_materialized_columns = 1;

ALTER TABLE mutation_matcher_late_cycle_materialize MATERIALIZE COLUMN m SETTINGS mutations_sync = 1; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_materialized_columns = 0;

CREATE TABLE mutation_matcher_late_cycle_clear
(
    a UInt8 DEFAULT 1,
    m String MATERIALIZED toJSONString(tuple(*))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_matcher_late_cycle_clear (a) VALUES (1);

SET asterisk_include_materialized_columns = 1;

ALTER TABLE mutation_matcher_late_cycle_clear CLEAR COLUMN a SETTINGS mutations_sync = 1; -- { serverError CYCLIC_ALIASES }

DROP TABLE mutation_matcher_late_cycle_clear;
DROP TABLE mutation_matcher_late_cycle_materialize;
