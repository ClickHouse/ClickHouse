-- Tags: no-random-settings

-- Regression test for the "Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8."
-- exception when using RIGHT JOIN with StorageJoin, USING clause, and QUALIFY,
-- with the legacy join planning path (`query_plan_use_new_logical_join_step` = 0).
--
-- The legacy code path did not add "Right Type Conversion Actions" after `FilledJoinStep`,
-- so the right-side USING key column remained non-nullable (UInt32) instead of being cast
-- to Nullable(UInt32). The QUALIFY filter then expected Nullable(UInt8) from `equals` but got UInt8.
--
-- https://github.com/ClickHouse/ClickHouse/issues/96101
-- https://github.com/ClickHouse/ClickHouse/issues/95678
-- https://github.com/ClickHouse/ClickHouse/issues/89802

SET enable_analyzer = 1;
SET query_plan_use_new_logical_join_step = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS right_join;

CREATE TABLE t1 (x Nullable(UInt32), str String) ENGINE = Memory;
CREATE TABLE right_join (x UInt32, s String) ENGINE = Join(ALL, RIGHT, x);

INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (NULL, 'c');
INSERT INTO right_join VALUES (1, 'A'), (3, 'C');

SELECT *
FROM t1 RIGHT JOIN right_join USING (x)
QUALIFY x = 1;

DROP TABLE t1;
DROP TABLE right_join;
