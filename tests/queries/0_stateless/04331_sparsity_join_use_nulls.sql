-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- Regression test: with `join_use_nulls = 1` the analyzer wraps the joined
-- column as `Nullable(...)` in the query view. The sparsity-filter classifier
-- must use the storage type, not the wrapped type, when looking up the
-- persisted `num_defaults` counter; otherwise `IS NULL` on a non-Nullable
-- source column matches the post-join NULLs but the counter only sees the
-- in-storage value of 0 and the pruner drops the whole part.

DROP TABLE IF EXISTS t_join_nulls_left;
DROP TABLE IF EXISTS t_join_nulls_right;

CREATE TABLE t_join_nulls_left  (col1 UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_join_nulls_right (col1 UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_join_nulls_left  VALUES (1), (2), (3);
INSERT INTO t_join_nulls_right VALUES (1), (2);

SET join_use_nulls = 1;

SELECT 'left_anti', l.col1
FROM t_join_nulls_left AS l
LEFT JOIN t_join_nulls_right AS r ON l.col1 = r.col1
WHERE r.col1 IS NULL
ORDER BY l.col1
SETTINGS use_sparsity_info_for_pruning = 'planning';

DROP TABLE t_join_nulls_left;
DROP TABLE t_join_nulls_right;
