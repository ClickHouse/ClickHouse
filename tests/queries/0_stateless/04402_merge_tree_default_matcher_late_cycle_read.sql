DROP TABLE IF EXISTS merge_tree_default_matcher_late_cycle_read;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE merge_tree_default_matcher_late_cycle_read
(
    a UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO merge_tree_default_matcher_late_cycle_read VALUES (1);

ALTER TABLE merge_tree_default_matcher_late_cycle_read
    ADD COLUMN b String DEFAULT toJSONString(tuple(* EXCEPT b));

ALTER TABLE merge_tree_default_matcher_late_cycle_read
    ADD COLUMN m String MATERIALIZED b;

SET asterisk_include_materialized_columns = 1;

SELECT b FROM merge_tree_default_matcher_late_cycle_read; -- { serverError CYCLIC_ALIASES }

DROP TABLE merge_tree_default_matcher_late_cycle_read;
