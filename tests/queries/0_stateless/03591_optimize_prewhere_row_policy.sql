-- {echoOn}

SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;
SET allow_statistics_optimize = 0;

DROP TABLE IF EXISTS 03591_test;

DROP ROW POLICY IF EXISTS 03591_rp ON 03591_test;

CREATE TABLE 03591_test (a Int32, b Int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO 03591_test VALUES (3, 1), (2, 2), (3, 2);

SELECT * FROM 03591_test;

SELECT * FROM 03591_test WHERE throwIf(b=1, 'Should throw') SETTINGS optimize_move_to_prewhere = 1; -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}

CREATE ROW POLICY 03591_rp ON 03591_test USING b=2 TO CURRENT_USER;

SELECT * FROM 03591_test;

-- Print plan with actions to make sure both a > 0 and b=2 are present in the prewhere section
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE a > 0 SETTINGS optimize_move_to_prewhere = 1, allow_experimental_analyzer = 1;
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE a > 0 SETTINGS optimize_move_to_prewhere = 1, allow_experimental_analyzer = 0;

SELECT * FROM 03591_test WHERE throwIf(b=1, 'Should not throw because b=1 is not visible to this user due to the b=2 row policy') SETTINGS optimize_move_to_prewhere = 1;

-- Print plan with actions to make sure a > 0, b = 2 and a = 3 are present in the prewhere section
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE a > 0 SETTINGS optimize_move_to_prewhere = 1, additional_table_filters={'03591_test': 'a=3'}, allow_experimental_analyzer = 1;
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE a > 0 SETTINGS optimize_move_to_prewhere = 1, additional_table_filters={'03591_test': 'a=3'}, allow_experimental_analyzer = 0;

DROP ROW POLICY 03591_rp ON 03591_test;

SELECT * FROM 03591_test WHERE throwIf(b=2, 'Should throw') SETTINGS optimize_move_to_prewhere = 1; -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}
