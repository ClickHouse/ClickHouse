SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET allow_statistic_optimize = 1;
SET query_plan_join_swap_table='auto';

-- R1: Small dimension table (Demo size: 10)
CREATE TABLE R1 (
    A_ID UInt32,
    A_Description String
) ENGINE = MergeTree()
PRIMARY KEY (A_ID)
SETTINGS auto_statistics_types = 'uniq';

-- R2: Large fact table (Demo size: 1,000)
-- Joins only with R1.
CREATE TABLE R2 (
    B_ID UInt32,
    R1_A_ID UInt32,
    B_Data Float64
) ENGINE = MergeTree()
PRIMARY KEY (B_ID)
SETTINGS auto_statistics_types = 'uniq';

-- R3: Another large fact table (Demo size: 1,000)
-- Joins with R1 and R4.
CREATE TABLE R3 (
    C_ID UInt32,
    R1_A_ID UInt32,
    R4_D_ID UInt32,
    C_Value Int32
) ENGINE = MergeTree()
PRIMARY KEY (C_ID)
SETTINGS auto_statistics_types = 'uniq';

-- R4: Small lookup table (Demo size: 10)
-- Joins only with R3.
CREATE TABLE R4 (
    D_ID UInt32,
    D_LookupCode String
) ENGINE = MergeTree()
PRIMARY KEY (D_ID)
SETTINGS auto_statistics_types = 'uniq';


-- Populate R1 (Small: 10 rows)
INSERT INTO R1 (A_ID, A_Description) VALUES
(1, 'Type A'), (2, 'Type B'), (3, 'Type C'), (4, 'Type D'), (5, 'Type E'),
(6, 'Type F'), (7, 'Type G'), (8, 'Type H'), (9, 'Type I'), (10, 'Type J');

-- Populate R4 (Small: 10 rows)
INSERT INTO R4 (D_ID, D_LookupCode) VALUES
(101, 'Lookup X'), (102, 'Lookup Y'), (103, 'Lookup Z'), (104, 'Lookup W'), (105, 'Lookup V'),
(106, 'Lookup U'), (107, 'Lookup T'), (108, 'Lookup S'), (109, 'Lookup R'), (110, 'Lookup Q');

INSERT INTO R2 (B_ID, R1_A_ID, B_Data)
SELECT
    number AS B_ID,
    (number % 10) + 1 AS R1_A_ID,  -- Links to R1.A_ID 1-10
    number / 100
FROM numbers(1000);

INSERT INTO R3 (C_ID, R1_A_ID, R4_D_ID, C_Value)
SELECT
    number AS C_ID,
    (number % 10) + 1 AS R1_A_ID,     -- Links to R1.A_ID 1-10
    (number % 10) + 101 AS R4_D_ID,   -- Links to R4.D_ID 101-110
    (number * 10) AS C_Value
FROM numbers(1000);


SELECT '=========================================';
SELECT 'Plan with greedy algorithm';
EXPLAIN
SELECT
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_parallel_replicas = 0;

SELECT sum(sipHash64(
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode))
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';


SELECT '=========================================';
SELECT 'Plan with DPsize algorithm';
EXPLAIN
SELECT
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

SELECT sum(sipHash64(
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode))
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize';


SELECT '===========================================';
SELECT 'Fallback to greedy';

SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas=0; --{serverError EXPERIMENTAL_FEATURE_ERROR}

SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy', enable_parallel_replicas=0;


