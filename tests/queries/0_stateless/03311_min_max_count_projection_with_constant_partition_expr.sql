DROP TABLE IF EXISTS t0;

-- Subqueries are not allowed in key expressions.
-- Previously this caused a LOGICAL_ERROR; now it's caught at table creation time.
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PARTITION BY (EXISTS (SELECT 1)) ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
