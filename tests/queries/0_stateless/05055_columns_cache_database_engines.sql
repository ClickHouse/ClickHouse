-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Tag no-replicated-database: the test creates its own databases with specific engines.

-- The columns cache is keyed by the table UUID, so it only works for tables that have one.
-- `Atomic` and `Replicated` databases give every table a UUID and therefore participate in the
-- cache, while `Ordinary` (and `Shared`, which cannot be created here) leave the UUID nil and
-- the cache is silently ignored for their tables. This test proves both sides of that contract.

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;
SET log_queries = 1;
SET send_logs_level = 'fatal';

-- =============================================================================
-- `Ordinary` database: nil UUID, the cache must stay completely out of the way.
-- =============================================================================

SELECT 'Ordinary database';

SET allow_deprecated_database_ordinary = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine
SELECT number, 'value_' || toString(number) FROM numbers(5000);

-- The table really has no UUID - this is the precondition the whole no-op path rests on.
SELECT uuid = toUUID('00000000-0000-0000-0000-000000000000')
FROM system.tables
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_cache_db_engine';

-- The parts are wide, so the only reason for the cache to stay away is the missing UUID.
SELECT DISTINCT part_type FROM system.parts
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_cache_db_engine' AND active;

SYSTEM DROP COLUMNS CACHE;

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine WHERE id < 3000
SETTINGS log_comment = '05055_ordinary_read1';

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine WHERE id < 3000
SETTINGS log_comment = '05055_ordinary_read2';

-- Nothing at all was cached: neither this table nor anything else got an entry.
SELECT count() FROM system.columns_cache;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- =============================================================================
-- `Replicated` database: the tables have a UUID, so the cache must work as usual.
-- =============================================================================

SELECT 'Replicated database';

-- Keep the per-replica status rows of the distributed DDL out of the test output. Every DDL below
-- is still proven to have taken effect by the queries that follow it.
SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/test/databases/' || currentDatabase() || '/05055_columns_cache', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine
SELECT number, 'value_' || toString(number) FROM numbers(5000);

SELECT uuid != toUUID('00000000-0000-0000-0000-000000000000')
FROM system.tables
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_cache_db_engine';

SYSTEM DROP COLUMNS CACHE;

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine WHERE id < 3000
SETTINGS log_comment = '05055_replicated_read1';

-- The first read populated the cache for this table.
SELECT count() > 0 FROM system.columns_cache
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_cache_db_engine';

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_cache_db_engine WHERE id < 3000
SETTINGS log_comment = '05055_replicated_read2';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- =============================================================================
-- Both directions of the eligibility check, seen through `ProfileEvents`:
-- the `Ordinary` reads neither hit nor missed the cache (it was never consulted),
-- while the repeated `Replicated` read was served from it.
-- =============================================================================

SELECT 'ProfileEvents checks';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('05055_ordinary_read1', '05055_ordinary_read2', '05055_replicated_read1', '05055_replicated_read2')
ORDER BY log_comment;
