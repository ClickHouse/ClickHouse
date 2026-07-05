-- Basic DPhyp correctness on a 4-table chain (R2 - R1 - R3 - R4).
-- R1 holds a single row, so its cardinality is known from the row count alone -
-- no single-table filter, no dependence on column statistics - and the optimal
-- plan is unique across environments. EXPLAIN pins it; the hash must match DPsize.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 0; -- pin (randomized in CI): statistics built on INSERT change the join order
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET explain_query_plan_default = 'legacy';

CREATE TABLE R1 (
    A_ID UInt32,
    A_Description String
) ENGINE = MergeTree()
PRIMARY KEY (A_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R2 (
    B_ID UInt32,
    R1_A_ID UInt32,
    B_Data Float64
) ENGINE = MergeTree()
PRIMARY KEY (B_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R3 (
    C_ID UInt32,
    R1_A_ID UInt32,
    R4_D_ID UInt32,
    C_Value Int32
) ENGINE = MergeTree()
PRIMARY KEY (C_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R4 (
    D_ID UInt32,
    D_LookupCode String
) ENGINE = MergeTree()
PRIMARY KEY (D_ID)
SETTINGS auto_statistics_types = 'uniq';

INSERT INTO R1 (A_ID, A_Description) VALUES (8, 'Type H');

INSERT INTO R4 (D_ID, D_LookupCode) VALUES
(101, 'Lookup X'), (102, 'Lookup Y'), (103, 'Lookup Z'), (104, 'Lookup W'), (105, 'Lookup V'),
(106, 'Lookup U'), (107, 'Lookup T'), (108, 'Lookup S'), (109, 'Lookup R'), (110, 'Lookup Q');

INSERT INTO R2 (B_ID, R1_A_ID, B_Data)
SELECT
    number AS B_ID,
    (number % 10) + 1 AS R1_A_ID,
    number / 100
FROM numbers(1000);

INSERT INTO R3 (C_ID, R1_A_ID, R4_D_ID, C_Value)
SELECT
    number AS C_ID,
    (number % 10) + 1 AS R1_A_ID,
    (number % 10) + 101 AS R4_D_ID,
    (number * 10) AS C_Value
FROM numbers(1000);

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
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPhyp result must match DPsize.
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
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

DROP TABLE R1;
DROP TABLE R2;
DROP TABLE R3;
DROP TABLE R4;
