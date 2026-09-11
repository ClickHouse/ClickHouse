-- Tags: distributed
--
-- Regression test for the `GLOBAL IN` external table population during primary key analysis.
--
-- For `GLOBAL IN`, the initiator materializes the subquery into a temporary (external) table that is
-- sent to the remote shards. Under the old analyzer this table is filled by the speculative in-place
-- set build (`FutureSetFromSubquery::buildOrderedSetInplace`) that runs during primary key analysis.
--
-- The external table must contain *every* row produced by the subquery, even when
-- `use_index_for_in_with_subqueries_max_values` is small: `Set::insertFromColumns` drops the explicit
-- in-memory set elements once that limit is exceeded (the set can still be used for membership tests,
-- but its element columns are gone). The external table is streamed from the subquery output, not
-- replayed from the (possibly dropped) set elements, so the membership result on the shards must stay
-- correct regardless of the limit.

DROP TABLE IF EXISTS t_glob_local SYNC;
DROP TABLE IF EXISTS t_glob_dist SYNC;

CREATE TABLE t_glob_local (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_glob_dist (a UInt64) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t_glob_local, rand());

-- Insert directly into the local table: both shards of `test_cluster_two_shards` resolve to this same
-- physical server, so every matching row is visible from both shards (hence the counts double).
INSERT INTO t_glob_local SELECT number FROM numbers(1000);

-- `a` is the primary key, so the `GLOBAL IN` triggers the in-place set build during index analysis.
-- The subquery returns 500 distinct values, far above `use_index_for_in_with_subqueries_max_values = 5`,
-- so the explicit set elements are dropped. The external table must still hold all 500 values; the
-- expected count is 500 matching rows x 2 shards = 1000.
SELECT count() FROM t_glob_dist WHERE a GLOBAL IN (SELECT number FROM numbers(500))
SETTINGS use_index_for_in_with_subqueries_max_values = 5;

SELECT count() FROM t_glob_dist WHERE a GLOBAL IN (SELECT number FROM numbers(500))
SETTINGS use_index_for_in_with_subqueries_max_values = 5, enable_analyzer = 0;

-- Same with the limit effectively disabled (explicit elements kept): the result must be identical.
SELECT count() FROM t_glob_dist WHERE a GLOBAL IN (SELECT number FROM numbers(500))
SETTINGS use_index_for_in_with_subqueries_max_values = 1000000;

SELECT count() FROM t_glob_dist WHERE a GLOBAL IN (SELECT number FROM numbers(500))
SETTINGS use_index_for_in_with_subqueries_max_values = 1000000, enable_analyzer = 0;

DROP TABLE t_glob_local SYNC;
DROP TABLE t_glob_dist SYNC;
