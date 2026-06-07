-- Tags: no-parallel
-- no-parallel: Uses the `future_set_from_subquery_skip_inplace_build` failpoint, which is global.
--
-- Regression test for `GLOBAL IN` when the in-place set build stops early.
--
-- For `GLOBAL IN`, the set used for index analysis reads from a temporary external
-- table that a separate "producer" `FutureSetFromSubquery` is responsible for
-- populating. If the producer's in-place build gives up (here forced via the
-- failpoint), the external table is left empty. The dependent set must NOT build
-- itself from the empty table and cache an empty set; it must defer to the delayed
-- path, which rebuilds the producer plan and fills the table first.

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

-- `GLOBAL IN` over `remote()` forces creation of a temporary external table for the
-- right-hand subquery, exercising the producer/dependent set dependency.

SET enable_analyzer = 1;
SYSTEM ENABLE FAILPOINT future_set_from_subquery_skip_inplace_build;
SELECT count() == 1
FROM remote('127.0.0.2', currentDatabase(), '04095_data')
WHERE key GLOBAL IN (SELECT key FROM 04095_keys);
SYSTEM DISABLE FAILPOINT future_set_from_subquery_skip_inplace_build;

SET enable_analyzer = 0;
SYSTEM ENABLE FAILPOINT future_set_from_subquery_skip_inplace_build;
SELECT count() == 1
FROM remote('127.0.0.2', currentDatabase(), '04095_data')
WHERE key GLOBAL IN (SELECT key FROM 04095_keys);
SYSTEM DISABLE FAILPOINT future_set_from_subquery_skip_inplace_build;

DROP TABLE 04095_keys;
DROP TABLE 04095_data;
