-- Test: CTE with SELECT * should enable subcolumn pruning
-- Before fix: CTE would project entire Tuple, preventing subcolumn pruning
-- After fix: SubcolumnPushdownPass pushes subcolumn access into CTE

DROP TABLE IF EXISTS test_subcolumn_pruning;

CREATE TABLE test_subcolumn_pruning (
    id UInt64,
    event Tuple(class_name String, category_name String, message String)
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_subcolumn_pruning VALUES (1, ('cls', 'cat', 'msg'));

-- Direct query reads only event.class_name
SELECT 'Direct query header:';
EXPLAIN header=1 SELECT event.class_name FROM test_subcolumn_pruning
SETTINGS enable_analyzer=1;

SELECT '';

-- CTE with SELECT * should also read only event.class_name (not entire event Tuple)
SELECT 'CTE with SELECT * header:';
EXPLAIN header=1
WITH foo AS (SELECT * FROM test_subcolumn_pruning)
SELECT event.class_name FROM foo
SETTINGS enable_analyzer=1;

DROP TABLE test_subcolumn_pruning;
