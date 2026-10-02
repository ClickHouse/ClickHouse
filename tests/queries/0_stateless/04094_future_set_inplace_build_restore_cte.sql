-- Tags: no-parallel
-- no-parallel: Uses the `prepared_sets_build_ordered_set_inplace_fail` failpoint, which is global.
--
-- Regression test: an `IN` subquery that reads a (non-materialized) CTE must still produce the
-- correct result when its speculative in-place set build for primary key analysis stops without
-- creating the set. The failpoint skips `Set::finishInsert` once, so the set is left not created
-- on the in-place pass, and the deferred build must create it from the preserved subquery plan.

DROP TABLE IF EXISTS 04094_data;
DROP TABLE IF EXISTS 04094_keys;

CREATE TABLE 04094_data
(
    key String,
    value UInt8
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE 04094_keys
(
    key String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO 04094_data VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO 04094_keys VALUES ('a'), ('x');

SET use_index_for_in_with_subqueries = 1;

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
WITH A AS (SELECT key FROM 04094_keys)
SELECT count() == 1
FROM 04094_data
WHERE key IN (SELECT key FROM A);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE 04094_keys;
DROP TABLE 04094_data;
