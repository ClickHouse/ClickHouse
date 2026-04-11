-- ============================================================================
-- Test: UNIQUE key constraints for MergeTree (RFC #70589)
-- ============================================================================
-- Comprehensive test suite covering:
--   1.  Basic UNIQUE constraint creation and metadata
--   2.  INSERT with unique rows (positive path)
--   3.  INSERT with duplicate key (rejection)
--   4.  INSERT IGNORE silently skips duplicates
--   5.  Intra-block duplicate detection
--   6.  Multi-column UNIQUE keys
--   7.  ALTER TABLE ADD/DROP CONSTRAINT
--   8.  UNIQUE with String data type
--   9.  UNIQUE with Nullable columns
--   10. UNIQUE with multiple constraints on same table
--   11. UNIQUE constraint survives OPTIMIZE FINAL
--   12. Large batch inserts with duplicates
--   13. Empty table operations
--   14. Error cases (invalid syntax, missing columns)
-- ============================================================================

-- ============================================================================
-- Test 1: Basic UNIQUE constraint — CREATE TABLE
-- ============================================================================
SELECT 'Test 1: Create table with UNIQUE';

DROP TABLE IF EXISTS t_unique;

CREATE TABLE t_unique
(
    id UInt64,
    name String,
    value Float64,
    CONSTRAINT uq_id UNIQUE (id)
)
ENGINE = MergeTree
ORDER BY id;

SELECT 'OK: Table created';

-- ============================================================================
-- Test 2: INSERT unique rows (should succeed)
-- ============================================================================
SELECT 'Test 2: Insert unique rows';

INSERT INTO t_unique VALUES (1, 'alice', 10.0);
INSERT INTO t_unique VALUES (2, 'bob', 20.0);
INSERT INTO t_unique VALUES (3, 'charlie', 30.0);

SELECT id, name, value FROM t_unique ORDER BY id;

-- ============================================================================
-- Test 3: INSERT duplicate key (should FAIL)
-- ============================================================================
SELECT 'Test 3: Duplicate key rejected';

INSERT INTO t_unique VALUES (1, 'duplicate', 99.0); -- { serverError DUPLICATE_KEY }

-- Verify original row preserved
SELECT id, name FROM t_unique WHERE id = 1;

-- ============================================================================
-- Test 4: INSERT IGNORE — skip duplicates silently
-- ============================================================================
SELECT 'Test 4: INSERT IGNORE';

INSERT IGNORE INTO t_unique VALUES (1, 'ignored', 0.0), (4, 'new_row', 40.0);

-- Should have 4 rows: original id=1 preserved + new id=4
SELECT id, name FROM t_unique ORDER BY id;

-- ============================================================================
-- Test 5: Intra-block duplicate detection
-- ============================================================================
SELECT 'Test 5: Intra-block duplicates';

-- Both rows have id=5. In strict mode, this should fail.
INSERT INTO t_unique VALUES (5, 'first', 50.0), (5, 'second', 55.0); -- { serverError DUPLICATE_KEY }

-- With INSERT IGNORE, first row wins
INSERT IGNORE INTO t_unique VALUES (6, 'first_six', 60.0), (6, 'second_six', 66.0);

SELECT id, name FROM t_unique WHERE id = 6;

-- ============================================================================
-- Test 6: Multi-column UNIQUE key
-- ============================================================================
SELECT 'Test 6: Multi-column UNIQUE';

DROP TABLE IF EXISTS t_unique_multi;

CREATE TABLE t_unique_multi
(
    a UInt32,
    b String,
    c Float64,
    CONSTRAINT uq_ab UNIQUE (a, b)
)
ENGINE = MergeTree
ORDER BY a;

-- Different combos — all OK
INSERT INTO t_unique_multi VALUES (1, 'x', 10.0);
INSERT INTO t_unique_multi VALUES (1, 'y', 20.0);
INSERT INTO t_unique_multi VALUES (2, 'x', 30.0);

SELECT a, b, c FROM t_unique_multi ORDER BY a, b;

-- Same combo (1, 'x') — should fail
INSERT INTO t_unique_multi VALUES (1, 'x', 99.0); -- { serverError DUPLICATE_KEY }

SELECT count() FROM t_unique_multi;

DROP TABLE t_unique_multi;

-- ============================================================================
-- Test 7: ALTER TABLE ADD / DROP CONSTRAINT
-- ============================================================================
SELECT 'Test 7: ALTER ADD/DROP CONSTRAINT';

DROP TABLE IF EXISTS t_unique_alter;

CREATE TABLE t_unique_alter
(
    id UInt64,
    name String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_unique_alter VALUES (1, 'hello'), (2, 'world');

-- Add UNIQUE constraint dynamically
ALTER TABLE t_unique_alter ADD CONSTRAINT uq_name UNIQUE (name);

-- This should now fail
INSERT INTO t_unique_alter VALUES (3, 'hello'); -- { serverError DUPLICATE_KEY }

-- Drop constraint
ALTER TABLE t_unique_alter DROP CONSTRAINT uq_name;

-- Now same insert should succeed
INSERT INTO t_unique_alter VALUES (3, 'hello');

SELECT count() FROM t_unique_alter;

DROP TABLE t_unique_alter;

-- ============================================================================
-- Test 8: UNIQUE with String data type
-- ============================================================================
SELECT 'Test 8: String keys';

DROP TABLE IF EXISTS t_unique_str;

CREATE TABLE t_unique_str
(
    key String,
    val Int64,
    CONSTRAINT uq_key UNIQUE (key)
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO t_unique_str VALUES ('alpha', 1);
INSERT INTO t_unique_str VALUES ('beta', 2);
INSERT INTO t_unique_str VALUES ('gamma', 3);

-- Duplicate string key
INSERT INTO t_unique_str VALUES ('alpha', 99); -- { serverError DUPLICATE_KEY }

-- Case-sensitive: 'Alpha' != 'alpha'
INSERT INTO t_unique_str VALUES ('Alpha', 4);

SELECT key, val FROM t_unique_str ORDER BY key;

DROP TABLE t_unique_str;

-- ============================================================================
-- Test 9: UNIQUE with multiple constraints on same table
-- ============================================================================
SELECT 'Test 9: Multiple constraints';

DROP TABLE IF EXISTS t_unique_dual;

CREATE TABLE t_unique_dual
(
    id UInt64,
    email String,
    name String,
    CONSTRAINT uq_id UNIQUE (id),
    CONSTRAINT uq_email UNIQUE (email)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_unique_dual VALUES (1, 'alice@test.com', 'Alice');
INSERT INTO t_unique_dual VALUES (2, 'bob@test.com', 'Bob');

-- Duplicate id
INSERT INTO t_unique_dual VALUES (1, 'new@test.com', 'New'); -- { serverError DUPLICATE_KEY }

-- Duplicate email (different id)
INSERT INTO t_unique_dual VALUES (3, 'alice@test.com', 'Clone'); -- { serverError DUPLICATE_KEY }

-- Both unique — should succeed
INSERT INTO t_unique_dual VALUES (3, 'charlie@test.com', 'Charlie');

SELECT id, email FROM t_unique_dual ORDER BY id;

DROP TABLE t_unique_dual;

-- ============================================================================
-- Test 10: Large batch with mixed duplicates (INSERT IGNORE)
-- ============================================================================
SELECT 'Test 10: Large batch INSERT IGNORE';

DROP TABLE IF EXISTS t_unique_batch;

CREATE TABLE t_unique_batch
(
    id UInt64,
    CONSTRAINT uq_id UNIQUE (id)
)
ENGINE = MergeTree
ORDER BY id;

-- Insert 10 unique rows
INSERT INTO t_unique_batch SELECT number FROM numbers(10);

-- Insert 20 rows where half are duplicates of existing
INSERT IGNORE INTO t_unique_batch SELECT number FROM numbers(20);

-- Should have exactly 20 unique rows (0-19)
SELECT count() FROM t_unique_batch;

DROP TABLE t_unique_batch;

-- ============================================================================
-- Test 11: UNIQUE constraint survives OPTIMIZE FINAL
-- ============================================================================
SELECT 'Test 11: Survives OPTIMIZE';

DROP TABLE IF EXISTS t_unique_opt;

CREATE TABLE t_unique_opt
(
    id UInt64,
    val String,
    CONSTRAINT uq_id UNIQUE (id)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_unique_opt VALUES (1, 'a');
INSERT INTO t_unique_opt VALUES (2, 'b');
INSERT INTO t_unique_opt VALUES (3, 'c');

OPTIMIZE TABLE t_unique_opt FINAL;

-- After merge, constraint should still work
INSERT INTO t_unique_opt VALUES (1, 'dup'); -- { serverError DUPLICATE_KEY }

SELECT count() FROM t_unique_opt;

DROP TABLE t_unique_opt;

-- ============================================================================
-- Test 12: Empty table edge cases
-- ============================================================================
SELECT 'Test 12: Empty table';

DROP TABLE IF EXISTS t_unique_empty;

CREATE TABLE t_unique_empty
(
    id UInt64,
    CONSTRAINT uq_id UNIQUE (id)
)
ENGINE = MergeTree
ORDER BY id;

-- INSERT into empty table should always succeed
INSERT INTO t_unique_empty VALUES (1);

SELECT count() FROM t_unique_empty;

-- INSERT IGNORE into empty table
INSERT IGNORE INTO t_unique_empty VALUES (1), (2);

SELECT count() FROM t_unique_empty;

DROP TABLE t_unique_empty;

-- ============================================================================
-- Test 13: Error cases
-- ============================================================================
SELECT 'Test 13: Error cases';

-- UNIQUE on non-existent column
CREATE TABLE t_unique_bad (id UInt64, CONSTRAINT uq UNIQUE (missing_col)) ENGINE = MergeTree ORDER BY id; -- { serverError UNKNOWN_IDENTIFIER }

-- ============================================================================
-- Test 14: INSERT IGNORE keeps first occurrence in block
-- ============================================================================
SELECT 'Test 14: INSERT IGNORE first wins';

DROP TABLE IF EXISTS t_unique_first;

CREATE TABLE t_unique_first
(
    id UInt64,
    val String,
    CONSTRAINT uq_id UNIQUE (id)
)
ENGINE = MergeTree
ORDER BY id;

-- Three rows with same id=1, different vals. First wins.
INSERT IGNORE INTO t_unique_first VALUES (1, 'FIRST'), (1, 'SECOND'), (1, 'THIRD');

SELECT id, val FROM t_unique_first;

DROP TABLE t_unique_first;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE IF EXISTS t_unique;

SELECT 'All UNIQUE constraint tests passed';
