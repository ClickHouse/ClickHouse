SET allow_experimental_row_type = 1;

-- A Row wrapper stands in for its source columns at read time, so its stored values
-- must never diverge from tuple(<sources>): explicit inserts into the wrapper are
-- rejected even with `insert_allow_materialized_columns`, and an existing column
-- cannot be promoted to a wrapper by ALTER.

DROP TABLE IF EXISTS row_wrapper_no_writes;

CREATE TABLE row_wrapper_no_writes (
    a UInt64,
    b UInt64,
    combined Row(a UInt64, b UInt64) MATERIALIZED tuple(a, b)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO row_wrapper_no_writes (a, b, combined) VALUES (1, 2, (100, 200)); -- { serverError ILLEGAL_COLUMN }
INSERT INTO row_wrapper_no_writes (a, b, combined) SETTINGS insert_allow_materialized_columns = 1 VALUES (1, 2, (100, 200)); -- { serverError ILLEGAL_COLUMN }

-- Inserting the source columns and reading through the wrapper still works.
INSERT INTO row_wrapper_no_writes (a, b) VALUES (1, 2);
SELECT a, b FROM row_wrapper_no_writes SETTINGS query_plan_use_row_wrappers = 1;

DROP TABLE row_wrapper_no_writes;

DROP TABLE IF EXISTS row_wrapper_no_promote;

CREATE TABLE row_wrapper_no_promote (
    a UInt64,
    b UInt64,
    combined Row(a UInt64, b UInt64)
) ENGINE = MergeTree ORDER BY a;

INSERT INTO row_wrapper_no_promote VALUES (1, 2, (100, 200));

-- The ordinary column already holds user-written values that differ from tuple(a, b).
ALTER TABLE row_wrapper_no_promote MODIFY COLUMN combined Row(a UInt64, b UInt64) MATERIALIZED tuple(a, b); -- { serverError BAD_ARGUMENTS }

-- Adding a fresh wrapper column is fine: no part stores it, so it is always computed.
ALTER TABLE row_wrapper_no_promote ADD COLUMN wrapped Row(a UInt64, b UInt64) MATERIALIZED tuple(a, b);
SELECT a, b FROM row_wrapper_no_promote SETTINGS query_plan_use_row_wrappers = 1;
SELECT wrapped FROM row_wrapper_no_promote;

-- Modifying a wrapper without changing what it wraps is still allowed.
ALTER TABLE row_wrapper_no_promote MODIFY COLUMN wrapped Row(a UInt64, b UInt64) MATERIALIZED tuple(a, b) COMMENT 'wrapper';

DROP TABLE row_wrapper_no_promote;
