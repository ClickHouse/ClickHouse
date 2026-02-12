-- Tests for rewrite of has([constant array], <expr>) -> <expr> IN (constant array)

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v String,
    a Array(UInt32)
)
Engine = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, concat('Number', toString(number)), [number, number + 1, number + 2] from numbers(10000);

SET optimize_rewrite_has_to_in = 1;

-- basic test - 5 rows
SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id);

-- basic test - 3 rows
SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v);

-- Use an expression
SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id / 5);

-- Use an expression
SELECT COUNT(*) FROM tab WHERE has(['Numb124', 'Numb', 'Number9999'], substr(v,1,4));

-- Confirm that FUNCTION in() is seen in plan
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id)
    ) t WHERE explain like '%FUNCTION in%';

-- Confirm that FUNCTION in() is seen in plan
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v)
    ) t WHERE explain like '%FUNCTION in%';

-- Confirm that primary key index is used to skip with has -> in rewrite
SELECT COUNT(*) FROM (
    EXPLAIN indexes=1 SELECT COUNT(*) FROM tab WHERE has([82333,900000,100011], id)
    ) t WHERE explain like '%Granules: 0%';

-- Non const array, FUNCTION in() should not be seen
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(a, 5)
    ) t WHERE explain like '%FUNCTION in%';

SET optimize_rewrite_has_to_in = 0;

-- FUNCTION has() will be retained, FUNCTION in() should not be seen
SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has([118, 2355, 898, 7882, 77], id)
    ) t WHERE explain like '%FUNCTION in%';

SELECT COUNT(*) FROM (
    EXPLAIN actions=1,header=1 SELECT COUNT(*) FROM tab WHERE has(['Number33', 'Number77', 'Number9999'], v)
    ) t WHERE explain like '%FUNCTION in%';

DROP TABLE tab;
