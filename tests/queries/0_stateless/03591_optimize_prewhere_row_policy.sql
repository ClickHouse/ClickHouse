SET use_query_condition_cache=0;

DROP TABLE IF EXISTS 03591_test;

DROP ROW POLICY IF EXISTS 03591_rp ON 03591_test;

CREATE TABLE 03591_test (a Int32, b Int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO 03591_test VALUES (1, 1), (2, 2);

SELECT * FROM 03591_test;

SELECT * FROM 03591_test WHERE throwIf(b=2, 'Should throw') SETTINGS optimize_move_to_prewhere = 1; -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}

CREATE ROW POLICY 03591_rp ON 03591_test USING b=1 TO CURRENT_USER;

SELECT * FROM 03591_test;

-- Print plan with actions to make sure both b=1 and b=2 are present in the prewhere section
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE b=2 SETTINGS optimize_move_to_prewhere = 1;

SELECT * FROM 03591_test WHERE throwIf(b=2, 'Should not throw because b=2 is not visible to this user due to the b=1 row policy') SETTINGS optimize_move_to_prewhere = 1;

DROP ROW POLICY 03591_rp ON 03591_test;

SELECT * FROM 03591_test WHERE throwIf(b=2, 'Should throw') SETTINGS optimize_move_to_prewhere = 1; -- {serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO}
