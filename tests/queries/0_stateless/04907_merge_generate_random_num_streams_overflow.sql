-- `Merge` can contain arbitrary table engines, not only `MergeTree` tables. `GenerateRandom`
-- creates one source per requested stream and does not reduce that count when the query has no
-- `LIMIT`. Verify that `Merge` rejects an absurd count before passing it to the child.

DROP TABLE IF EXISTS t_merge_generate_random;
DROP TABLE IF EXISTS t_generate_random_child;

CREATE TABLE t_generate_random_child (n UInt64) ENGINE = GenerateRandom(1);
CREATE TABLE t_merge_generate_random (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_generate_random_child$');

SELECT n FROM t_merge_generate_random
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1073741824
FORMAT Null; -- { serverError PARAMETER_OUT_OF_BOUND }

DROP TABLE t_merge_generate_random;
DROP TABLE t_generate_random_child;
