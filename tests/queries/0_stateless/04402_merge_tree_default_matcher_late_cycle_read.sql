-- Matcher expansion in stored expressions is settings-independent. A `COLUMNS`-based
-- DEFAULT that would become cyclic through a later ALTER is rejected at ALTER time, and
-- reading a `*`-based DEFAULT of a missing column from a `MergeTree` part yields the same
-- value regardless of `asterisk_include_*` settings.
DROP TABLE IF EXISTS merge_tree_default_matcher_read;

CREATE TABLE merge_tree_default_matcher_read
(
    a UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO merge_tree_default_matcher_read VALUES (1);

ALTER TABLE merge_tree_default_matcher_read
    ADD COLUMN b String DEFAULT toJSONString(tuple(* EXCEPT b));

ALTER TABLE merge_tree_default_matcher_read
    ADD COLUMN m String MATERIALIZED b;

SET asterisk_include_materialized_columns = 1;

-- `b` is missing in the existing part, so its DEFAULT is computed at read time;
-- `*` never includes the MATERIALIZED column `m`, so there is no cycle and the result
-- does not depend on the session setting.
SELECT b FROM merge_tree_default_matcher_read;

-- The `COLUMNS` analogue of the cycle is rejected eagerly at ALTER time.
ALTER TABLE merge_tree_default_matcher_read
    ADD COLUMN c String DEFAULT toJSONString(tuple(COLUMNS('^(a|z)$')));
ALTER TABLE merge_tree_default_matcher_read
    ADD COLUMN z String MATERIALIZED c; -- { serverError CYCLIC_ALIASES }

DROP TABLE merge_tree_default_matcher_read;
