DROP TABLE IF EXISTS 03591_test;

DROP ROW POLICY IF EXISTS 03591_rp ON 03591_test;

CREATE TABLE 03591_test (a Int32, b Int32) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO 03591_test VALUES (1, 2);

CREATE ROW POLICY 03591_rp ON 03591_test USING a=1 TO CURRENT_USER;

-- Print plan with actions to make sure both a=1 and b=2 are present in the prewhere section
EXPLAIN PLAN actions=1 SELECT * FROM 03591_test WHERE b=2 SETTINGS optimize_move_to_prewhere = 1;
