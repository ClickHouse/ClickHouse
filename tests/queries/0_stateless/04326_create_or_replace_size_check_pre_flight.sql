-- Tags: no-replicated-database, no-shared-merge-tree
-- ^ Atomic database is required for CREATE OR REPLACE.

DROP TABLE IF EXISTS t04326;

CREATE TABLE t04326 (a UInt64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t04326 SELECT number FROM numbers(1000);

-- Pre-flight check fires BEFORE creating the temporary table, so the original
-- `t04326` keeps its data and no `_tmp_replace_*` is left behind. Before the
-- fix the size guard fired AFTER EXCHANGE, leaving the user with an empty
-- replacement and a stranded `_tmp_replace_*` table holding the data.
CREATE OR REPLACE TABLE t04326 (b UInt64) ENGINE = MergeTree() ORDER BY b
SETTINGS max_table_size_to_drop = 1;  -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }

-- The original `a` column and 1000 rows must still be there.
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't04326' ORDER BY name;
SELECT count() FROM t04326;

-- And no `_tmp_replace_*` table should be left behind.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

-- Bare REPLACE TABLE goes through the same path; it must also fail early
-- with the same guarantee.
REPLACE TABLE t04326 (c UInt64) ENGINE = MergeTree() ORDER BY c
SETTINGS max_table_size_to_drop = 1;  -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't04326' ORDER BY name;
SELECT count() FROM t04326;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

-- A `CREATE OR REPLACE` with adequate quota succeeds and leaves no temporary
-- table behind.
CREATE OR REPLACE TABLE t04326 (b UInt64) ENGINE = MergeTree() ORDER BY b
SETTINGS max_table_size_to_drop = 0;

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't04326' ORDER BY name;
SELECT count() FROM t04326;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE t04326;
