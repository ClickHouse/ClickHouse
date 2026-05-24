-- Tags: no-fasttest
-- Negative cases: Row(...) wrapper validation rejects malformed declarations.

DROP TABLE IF EXISTS row_validation;

-- Duplicate field name inside a Row(...) is rejected at type-parse time
-- (i.e. as soon as the CREATE TABLE statement is parsed).
SELECT 'expected DUPLICATE_COLUMN:';
CREATE TABLE row_validation (
    a String,
    b Row(x String, x UInt32) MATERIALIZED tuple(a, 0)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError DUPLICATE_COLUMN }

-- Empty Row(...) is rejected at type-parse time.
SELECT 'expected BAD_ARGUMENTS:';
CREATE TABLE row_validation (
    a String,
    b Row() MATERIALIZED tuple()
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
