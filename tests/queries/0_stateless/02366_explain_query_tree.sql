SET enable_analyzer = 1;

EXPLAIN QUERY TREE run_passes = 0 SELECT 1;

SELECT '--';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

EXPLAIN QUERY TREE run_passes = 0 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 SELECT arrayMap(x -> x + id, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT arrayMap(x -> x + 1, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

DROP TABLE test_table;

-- Coverage for SortNode.cpp dumpTreeImpl branches (lines 23, 50-51, 64-86) never called by
-- existing CI tests: DESCENDING direction, NULLS FIRST/LAST, WITH FILL FROM/TO/STEP/STALENESS.

SELECT '--';

-- 1. DESCENDING sort — hits line 23 (case SortDirection::DESCENDING: return "DESCENDING")
EXPLAIN QUERY TREE SELECT number FROM numbers(5) ORDER BY number DESC;

SELECT '--';

-- 2. NULLS FIRST — hits lines 50-51 (nulls_sort_direction branch)
EXPLAIN QUERY TREE SELECT number FROM numbers(5) ORDER BY number ASC NULLS FIRST;

SELECT '--';

-- 3. WITH FILL FROM/TO/STEP — hits lines 64-80
EXPLAIN QUERY TREE SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 1 TO 5 STEP 1;

SELECT '--';

-- 4. WITH FILL STALENESS — hits lines 82-86
EXPLAIN QUERY TREE SELECT number FROM numbers(10) ORDER BY number WITH FILL FROM 0 TO 5 STEP 1 STALENESS 2;
