-- Tags: no-random-merge-tree-settings

-- Test that duplicate columns in GROUP BY clause of projection are forbidden
-- This previously caused a crash (std::length_error) in RowOrderOptimizer::optimize
-- when optimize_row_order = 1 due to unsigned integer underflow

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t;

-- Original reproducer: this should fail at table creation, not crash during INSERT
CREATE TABLE t0 (c0 Int, PROJECTION p0 (SELECT c0 GROUP BY c0, c0)) ENGINE = MergeTree() PRIMARY KEY tuple() SETTINGS optimize_row_order = 1, allow_suspicious_indices = 1; -- { serverError ILLEGAL_PROJECTION }

-- This should fail with ILLEGAL_PROJECTION error due to duplicate column c0 in GROUP BY
-- allow_suspicious_indices = 1 bypasses the generic duplicate index check to test projection-specific validation
CREATE TABLE t (c0 Int, PROJECTION p0 (SELECT c0 GROUP BY c0, c0)) ENGINE = MergeTree() PRIMARY KEY tuple() SETTINGS allow_suspicious_indices = 1; -- { serverError ILLEGAL_PROJECTION }

-- Test with multiple columns, one duplicated
CREATE TABLE t (c0 Int, c1 Int, PROJECTION p0 (SELECT c0, c1 GROUP BY c0, c1, c0)) ENGINE = MergeTree() PRIMARY KEY tuple() SETTINGS allow_suspicious_indices = 1; -- { serverError ILLEGAL_PROJECTION }

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t;
