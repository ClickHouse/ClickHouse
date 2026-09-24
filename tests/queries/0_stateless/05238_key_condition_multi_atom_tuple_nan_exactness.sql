-- Tags: no-random-settings, no-random-merge-tree-settings
-- The read counts depend on the granularity and on implicit projections being enabled.

SET explain_query_plan_default = 'legacy';
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS test_multi_atom_tuple_nan;
CREATE TABLE test_multi_atom_tuple_nan (i UInt16, t Tuple(Float64, Int32)) ENGINE = MergeTree
ORDER BY (toUInt8(i), i, t)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_multi_atom_tuple_nan VALUES
    (257, (1., 1)), (257, (2., 1)), (257, (10., 1)), (257, (500., 7)),
    (257, (nan, 2)), (257, (nan, 3)), (257, (nan, 4)), (257, (nan, 5));

-- The exact atom on `i` covers its relaxed `toUInt8(i)` sibling. The independent tuple range
-- still needs row filtering: its index bounds can contain NaNs that fail the row comparison.
-- Exactness derived from the multi-atom group must respect that additional relaxation.
SELECT trimLeft(explain) FROM
    (EXPLAIN indexes = 1 SELECT count() FROM test_multi_atom_tuple_nan WHERE i = 257 AND t >= (5., 3))
WHERE explain LIKE '%Condition:%';

SELECT count() FROM test_multi_atom_tuple_nan WHERE i = 257 AND t >= (5., 3)
SETTINGS log_comment = '05238 tuple NaN filtering';
SELECT count() FROM test_multi_atom_tuple_nan WHERE i = 257 AND t >= (5., 3)
SETTINGS optimize_use_implicit_projections = 0;

-- Counting the tuple range reads rows, while the exact group alone can still use the index.
SELECT count() FROM test_multi_atom_tuple_nan WHERE i = 257
SETTINGS log_comment = '05238 exact group';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows > 1 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05238 tuple NaN filtering', '05238 exact group')
ORDER BY log_comment;

DROP TABLE test_multi_atom_tuple_nan;
