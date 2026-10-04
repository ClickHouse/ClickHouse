-- Tags: no-parallel, no-fasttest, shard
-- no-parallel: Uses the `prepared_sets_build_ordered_set_inplace_fail` failpoint, which is global.
-- no-fasttest: tests that run alone (because of the failpoint) are kept out of the fast test.
-- shard: Uses `remote('127.0.0.{1,2}', ...)`.
--
-- Regression test for `GLOBAL IN` when the speculative in-place set build stops early.
--
-- The local shard runs primary key analysis on the initiator, which builds the `GLOBAL IN` set in
-- place. The failpoint skips `Set::finishInsert` once, so that build stops without creating the set.
-- The deferred build must then create the set from the preserved subquery plan, and the temporary
-- external table sent to the remote shard must still contain the subquery result: an empty set or
-- table would make `count()` lower than expected.

DROP TABLE IF EXISTS 04095_data;
DROP TABLE IF EXISTS 04095_keys;

CREATE TABLE 04095_data
(
    key String,
    value UInt8
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE 04095_keys
(
    key String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO 04095_data VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO 04095_keys VALUES ('a'), ('x');

SET use_index_for_in_with_subqueries = 1;

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() == 2
FROM remote('127.0.0.{1,2}', currentDatabase(), '04095_data')
WHERE key GLOBAL IN (SELECT key FROM 04095_keys);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE 04095_keys;
DROP TABLE 04095_data;
