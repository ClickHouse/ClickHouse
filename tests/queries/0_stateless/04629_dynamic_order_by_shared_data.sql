-- Tags: no-fasttest
-- no-fasttest: requires the Dynamic data type.

-- Focused coverage for the ColumnDynamic side of the shared-data comparison optimization. The
-- both-shared branch of ColumnDynamic::doCompareAt (left_discr == right_discr == shared_variant)
-- now delegates to the shared helper ColumnDynamic::compareSerializedValues. Dynamic(max_types = 0)
-- forces every non-NULL value into shared data, so ORDER BY over such a column drives that branch
-- directly (JSON in 04628 only reaches it through ColumnObject). This pins the order so
-- it stays identical to the pre-refactor materializing implementation, and covers Dynamic-only
-- cases JSON cannot: NaN / Inf Float64 and LowCardinality(String).

SET allow_suspicious_types_in_order_by = 1;

-- 1. Single part, all values in shared data. Covers >= 2 shared rows of the SAME type (Int64 10/10/20,
-- Float64, String, LowCardinality(String)) and >= 2 of DIFFERENT types (Array/Float64/Int64/String).
-- The two Int64 10 rows are byte-identical, so they compare equal (0) and fall back to the id
-- tiebreak. NULL is the None discriminator (handled before the both-shared branch) and sorts last.
-- LowCardinality(String) keeps its own type name in shared data: its rows sort among themselves on
-- the same-type-name concrete branch and as a group separately from String on the type-name branch.
DROP TABLE IF EXISTS t_dyn_shared;
CREATE TABLE t_dyn_shared (id UInt32, d Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_shared VALUES
    (1,  CAST(10, 'Int64')),
    (2,  CAST(20, 'Int64')),
    (3,  CAST(10, 'Int64')),                    -- byte-equal to row 1
    (4,  CAST(1.5, 'Float64')),
    (5,  CAST(2.5, 'Float64')),
    (6,  'apple'::String),
    (7,  'banana'::String),
    (8,  'cherry'::LowCardinality(String)),     -- three LowCardinality(String) rows sort among
    (9,  'apple'::LowCardinality(String)),      -- themselves (same-type-name concrete branch) and
    (10, 'banana'::LowCardinality(String)),     -- as a group separately from String
    (11, CAST([1, 2, 3], 'Array(Int64)')),
    (12, CAST([1, 2], 'Array(Int64)')),
    (13, CAST(NULL, 'Dynamic(max_types = 0)'));
SELECT 'asc';
SELECT id, dynamicType(d) AS type, toString(d) AS value FROM t_dyn_shared ORDER BY d ASC, id ASC;
SELECT 'desc';
SELECT id, dynamicType(d) AS type, toString(d) AS value FROM t_dyn_shared ORDER BY d DESC, id ASC;
DROP TABLE t_dyn_shared;

-- 2. Float64-only shared data: NaN / +Inf / -Inf ordering goes through the same-type-name branch
-- (deserialize both into one Float64 column, compareAt with nan_direction_hint). ASC puts NaN last.
DROP TABLE IF EXISTS t_dyn_float;
CREATE TABLE t_dyn_float (id UInt32, d Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_float VALUES
    (1, CAST(nan, 'Float64')),
    (2, CAST(inf, 'Float64')),
    (3, CAST(-inf, 'Float64')),
    (4, CAST(1.5, 'Float64')),
    (5, CAST(0.0, 'Float64'));
SELECT 'float';
SELECT id, toString(d) AS value FROM t_dyn_float ORDER BY d ASC, id ASC;
DROP TABLE t_dyn_float;

-- 3. Cross-part merge: two parts, all shared, compared across two ColumnDynamic instances by
-- MergingSortedAlgorithm (not an in-block sort). Same-type pairs (Int64) and different-type pairs
-- (Float64 / String) are split across parts and interleaved so adjacent-in-order rows are cross-part.
DROP TABLE IF EXISTS t_dyn_parts;
CREATE TABLE t_dyn_parts (id UInt32, d Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_dyn_parts;
INSERT INTO t_dyn_parts VALUES (1, CAST(30, 'Int64')), (2, CAST(10, 'Int64')), (3, 'zed'::String);
INSERT INTO t_dyn_parts VALUES (4, CAST(20, 'Int64')), (5, CAST(1.5, 'Float64')), (6, 'abc'::String);
SELECT 'merge';
SELECT id, dynamicType(d) AS type, toString(d) AS value FROM t_dyn_parts
ORDER BY d ASC, id ASC SETTINGS max_threads = 2, max_block_size = 2;
DROP TABLE t_dyn_parts;
