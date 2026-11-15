-- A subcolumn that exists only in the type `Merge` derives for a column (`arr.null` of
-- `Array(Nullable(UInt16))`, derived from children declaring `Array(UInt8)` and `Array(Nullable(UInt16))`)
-- must be computed, not defaulted, by a child that runs the query above `FetchColumns` itself.

DROP TABLE IF EXISTS t_05258_a;
DROP TABLE IF EXISTS t_05258_b;
DROP TABLE IF EXISTS t_05258_dist_a;

CREATE TABLE t_05258_a (id UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05258_b (id UInt64, arr Array(Nullable(UInt16))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05258_a VALUES (1, [1, 2, 3]);
INSERT INTO t_05258_b VALUES (2, [NULL, 5]);
CREATE TABLE t_05258_dist_a AS t_05258_a ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05258_a);

SELECT id, arr.null FROM merge(currentDatabase(), '^t_05258_(dist_a|b)$') ORDER BY id;
SELECT sum(length(arr.null)), sum(arraySum(arr.null)) FROM merge(currentDatabase(), '^t_05258_(dist_a|b)$');
SELECT count() FROM merge(currentDatabase(), '^t_05258_(dist_a|b)$') WHERE has(arr.null, 0);

DROP TABLE t_05258_dist_a;
DROP TABLE t_05258_a;
DROP TABLE t_05258_b;
