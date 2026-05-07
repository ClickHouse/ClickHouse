SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table='auto';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

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

-- R5..R10: Small lookup tables used to test auto algorithm threshold with 9/10-way joins.
CREATE TABLE R5 (
    E_ID UInt32,
    E_Data String
) ENGINE = MergeTree()
PRIMARY KEY (E_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R6 (
    F_ID UInt32,
    F_Data String
) ENGINE = MergeTree()
PRIMARY KEY (F_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R7 (
    G_ID UInt32,
    G_Data String
) ENGINE = MergeTree()
PRIMARY KEY (G_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R8 (
    H_ID UInt32,
    H_Data String
) ENGINE = MergeTree()
PRIMARY KEY (H_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R9 (
    I_ID UInt32,
    I_Data String
) ENGINE = MergeTree()
PRIMARY KEY (I_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R10 (
    J_ID UInt32,
    J_Data String
) ENGINE = MergeTree()
PRIMARY KEY (J_ID)
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

INSERT INTO R5
SELECT number + 1, 'E-' || toString(number + 1)
FROM numbers(10);

INSERT INTO R6
SELECT number + 1, 'F-' || toString(number + 1)
FROM numbers(10);

INSERT INTO R7
SELECT number + 1, 'G-' || toString(number + 1)
FROM numbers(10);

INSERT INTO R8
SELECT number + 1, 'H-' || toString(number + 1)
FROM numbers(10);

INSERT INTO R9
SELECT number + 1, 'I-' || toString(number + 1)
FROM numbers(10);

INSERT INTO R10
SELECT number + 1, 'J-' || toString(number + 1)
FROM numbers(10);


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

SELECT '=========================================';
SELECT 'Auto algorithm selection at 9 and 10 tables';

SELECT 'Join order signature with auto algorithm at 9 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'auto',
        enable_parallel_replicas = 0
);

SELECT 'Join order signature with DPsize algorithm at 9 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'dpsize',
        enable_parallel_replicas = 0
);

SELECT 'Join order signature with greedy algorithm at 9 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'greedy',
        enable_parallel_replicas = 0
);

SELECT 'Join order signature with auto algorithm at 10 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9, R10 AS T10
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_ID = T10.J_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'auto',
        enable_parallel_replicas = 0
);

SELECT 'Join order signature with greedy algorithm at 10 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9, R10 AS T10
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_ID = T10.J_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'greedy',
        enable_parallel_replicas = 0
);

SELECT 'Join order signature with DPsize algorithm at 10 tables';
SELECT arrayStringConcat(
    arrayMap(
        x -> replaceRegexpAll(replaceRegexpAll(x, '^ReadFromMergeTree \\(', ''), '\\)$', ''),
        extractAll(arrayStringConcat(groupArray(explain), '\n'), 'ReadFromMergeTree \\(default\\.[^)]+\\)')
    ),
    ' '
)
FROM
(
    EXPLAIN
    SELECT count()
    FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4, R5 AS T5, R6 AS T6, R7 AS T7, R8 AS T8, R9 AS T9, R10 AS T10
    WHERE
        T1.A_ID = T2.R1_A_ID
        AND T1.A_ID = T3.R1_A_ID
        AND T3.R4_D_ID = T4.D_ID
        AND T1.A_ID = T5.E_ID
        AND T1.A_ID = T6.F_ID
        AND T1.A_ID = T7.G_ID
        AND T1.A_ID = T8.H_ID
        AND T1.A_ID = T9.I_ID
        AND T1.A_ID = T10.J_ID
        AND T1.A_Description = 'Type H'
        AND T4.D_LookupCode = 'Lookup S'
    SETTINGS
        query_plan_optimize_join_order_algorithm = 'dpsize',
        enable_parallel_replicas = 0
);


SELECT '===========================================';
SELECT 'Fallback to greedy';

-- Reset to greedy-only so that setting dpsize alone (without greedy fallback) triggers the experimental error check.
-- If the session already has dpsize in query_plan_optimize_join_order_algorithm (e.g. injected by the test runner),
-- the validation is skipped because dpsize is already considered enabled.
SET query_plan_optimize_join_order_algorithm = 'greedy';

SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas=0; --{serverError EXPERIMENTAL_FEATURE_ERROR}

SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy', enable_parallel_replicas=0;


