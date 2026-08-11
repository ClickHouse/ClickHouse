-- Tags: no-ordinary-database, no-replicated-database
-- no-ordinary-database: CREATE OR REPLACE requires an Atomic database.
-- no-replicated-database: POPULATE is not supported in a Replicated database.

-- The DROPs that CREATE OR REPLACE issues internally (cleaning up its temporary table on failure,
-- and dropping the replaced table after the swap) are steps of one user statement, not user DROPs,
-- so `ignore_drop_queries_probability` must not skip or rewrite them. When it does, the temporary
-- table is stranded as a `_tmp_replace_*` table that is invisible to the user, holds its data and
-- survives a restart, and one is leaked per statement.

SET ignore_drop_queries_probability = 1;

SELECT '-- failure path: the temporary table must be cleaned up, not stranded';

DROP TABLE IF EXISTS dst_04796 SYNC;
CREATE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO dst_04796 SELECT number FROM numbers(10);

-- One row per block, so blocks 0..49 land as parts in the temporary table before `throwIf` fires on
-- row 50: the statement then fails with the temporary table already created and populated, which is
-- the catch-block cleanup path.
CREATE OR REPLACE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a
AS SELECT throwIf(number = 50, 'stop') AS a FROM numbers(100)
SETTINGS max_insert_block_size = 1, min_insert_block_size_rows = 1,
         min_insert_block_size_bytes = 1, max_block_size = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';
-- The failed replace must leave the original table untouched.
SELECT count() FROM dst_04796;

SELECT '-- success path: the replaced table must be dropped, not left behind';

CREATE OR REPLACE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a
AS SELECT number AS a FROM numbers(3);

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';
SELECT count() FROM dst_04796;

SELECT '-- no leak accumulates across repeated replaces';

CREATE OR REPLACE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT number AS a FROM numbers(3);
CREATE OR REPLACE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT number AS a FROM numbers(3);
CREATE OR REPLACE TABLE dst_04796 (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT number AS a FROM numbers(3);

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE dst_04796 SYNC;

SELECT '-- a view temporary is not MergeTree, so its internal DROP would be rewritten to TRUNCATE';

-- A rewritten internal DROP takes an exclusive lock on the temporary storage under the outer
-- statement's query id (which already holds read locks on it), so it can also raise
-- `RWLockImpl::getLock(): Cannot acquire exclusive lock while RWLock is already locked`.
DROP TABLE IF EXISTS src_04796 SYNC;
DROP TABLE IF EXISTS mv_04796 SYNC;

CREATE TABLE src_04796 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_04796 SELECT number FROM numbers(100);
CREATE MATERIALIZED VIEW mv_04796 ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src_04796;

CREATE OR REPLACE MATERIALIZED VIEW mv_04796 ENGINE = MergeTree ORDER BY id POPULATE
AS SELECT throwIf(id = 50, 'stop') AS id FROM src_04796
SETTINGS max_insert_block_size = 1, min_insert_block_size_rows = 1,
         min_insert_block_size_bytes = 1, max_block_size = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';
-- The failed replace must leave the original view working.
SELECT count() FROM mv_04796;

SELECT '-- a user DROP is still skipped: the setting keeps doing what it is for';

DROP TABLE IF EXISTS user_drop_04796 SYNC;
CREATE TABLE user_drop_04796 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO user_drop_04796 SELECT number FROM numbers(10);
DROP TABLE user_drop_04796;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'user_drop_04796';

DROP TABLE mv_04796 SYNC;
DROP TABLE src_04796 SYNC;
DROP TABLE IF EXISTS user_drop_04796 SYNC SETTINGS ignore_drop_queries_probability = 0;
