-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- ^ Atomic database is required for CREATE OR REPLACE.

DROP TABLE IF EXISTS t04326;

CREATE TABLE t04326 (a UInt64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t04326 SELECT number FROM numbers(1000);

-- Pre-flight check fires after the temporary replacement is created and filled,
-- but BEFORE the destructive `EXCHANGE`, so the original `t04326` keeps its data
-- and no `_tmp_replace_*` is left behind. Before the fix the size guard fired
-- AFTER `EXCHANGE`, leaving the user with an empty replacement and a stranded
-- `_tmp_replace_*` table holding the data.
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

-- The pre-flight must use a side-effect-free size-only API so that engines whose
-- `checkTableCanBeDropped` override has unrelated semantics (rejecting DROP TABLE
-- on dictionaries; latching a broker-cleanup flag on NATS/RabbitMQ) are not
-- disturbed. Exercise the dictionary path: REPLACE DICTIONARY must still succeed
-- against an existing `StorageDictionary` whose `checkTableCanBeDropped` would
-- otherwise throw `CANNOT_DETACH_DICTIONARY_AS_TABLE`.
CREATE TABLE t04326_src (id UInt64, value String) ENGINE = TinyLog;
INSERT INTO t04326_src VALUES (0, 'v0');

CREATE OR REPLACE DICTIONARY t04326_dict (id UInt64, value String)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 't04326_src'));

SELECT * FROM t04326_dict;

CREATE OR REPLACE DICTIONARY t04326_dict (id UInt64, value String)
PRIMARY KEY id
LAYOUT(HASHED())
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 't04326_src'))
LIFETIME(0);

SELECT * FROM t04326_dict;

DROP DICTIONARY t04326_dict;
DROP TABLE t04326_src;

-- A `CREATE OR REPLACE MATERIALIZED VIEW` that owns an over-limit inner
-- `MergeTree` table must fail the size guard for the same reason a plain
-- `DROP TABLE mv` does (see `03667_drop_inner_table_size_limits.sql`):
-- replace must not be a privileged path that bypasses what plain drop refuses.
CREATE TABLE t04326_mv_src (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE MATERIALIZED VIEW t04326_mv (id UInt64) ENGINE = MergeTree() ORDER BY id
AS SELECT id FROM t04326_mv_src;
INSERT INTO t04326_mv_src SELECT number FROM numbers(1000);

CREATE OR REPLACE MATERIALIZED VIEW t04326_mv (id UInt64) ENGINE = MergeTree() ORDER BY id
AS SELECT id FROM t04326_mv_src
SETTINGS max_table_size_to_drop = 1;  -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }

-- The view and its inner table must both still be there.
SELECT count() FROM t04326_mv;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE t04326_mv SETTINGS max_table_size_to_drop = 0;
DROP TABLE t04326_mv_src;

-- Success path for `CREATE OR REPLACE TABLE ... AS SELECT`: the size check
-- now runs after the fill, but the old target is small enough so the guard
-- still passes and the swap completes.
CREATE TABLE t04326_select (a UInt64) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t04326_select VALUES (0);

CREATE OR REPLACE TABLE t04326_select (b UInt64) ENGINE = MergeTree() ORDER BY b
AS SELECT number FROM numbers(100);

SELECT count() FROM t04326_select;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't04326_select' ORDER BY name;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE t04326_select;

-- The `CREATE OR REPLACE MATERIALIZED VIEW` success path with a small inner
-- still works, even with a strict `max_table_size_to_drop`. The pre-flight
-- now runs after the fill but the old inner is small, so the guard passes
-- and the swap completes without leaving a stranded `_tmp_replace_*`.
CREATE TABLE t04326_mv2_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t04326_mv2 (id UInt64) ENGINE = MergeTree ORDER BY id
AS SELECT id FROM t04326_mv2_src;
INSERT INTO t04326_mv2_src VALUES (1);

CREATE OR REPLACE MATERIALIZED VIEW t04326_mv2 (id UInt64) ENGINE = MergeTree ORDER BY id
AS SELECT id FROM t04326_mv2_src
SETTINGS max_table_size_to_drop = 0;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%tmp_replace%';

DROP TABLE t04326_mv2 SETTINGS max_table_size_to_drop = 0;
DROP TABLE t04326_mv2_src;
