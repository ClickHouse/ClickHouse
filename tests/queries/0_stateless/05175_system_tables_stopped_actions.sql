-- Tags: atomic-database, memory-engine

CREATE TABLE stopped_actions_a (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_b (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_memory (x UInt64) ENGINE = Memory;

SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'tables' AND name = 'stopped_actions';
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name;

SYSTEM STOP MERGES stopped_actions_a;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_a';
SYSTEM STOP MERGES stopped_actions_a;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_a';

-- Insertion order must not affect the sorted, distinct action names.
SYSTEM STOP TTL MERGES stopped_actions_a;
SYSTEM STOP MOVES stopped_actions_a;
SYSTEM STOP CLEANUP stopped_actions_a;
SYSTEM STOP MOVES stopped_actions_b;
SYSTEM STOP MERGES stopped_actions_memory;
SYSTEM STOP TTL MERGES stopped_actions_memory;
SYSTEM STOP MOVES stopped_actions_memory;
SYSTEM STOP FETCHES stopped_actions_memory;
SYSTEM STOP REPLICATED SENDS stopped_actions_memory;
SYSTEM STOP REPLICATION QUEUES stopped_actions_memory;
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_memory;
SYSTEM STOP PULLING REPLICATION LOG stopped_actions_memory;
SYSTEM STOP CLEANUP stopped_actions_memory;
SYSTEM STOP VIEW stopped_actions_memory;
SYSTEM PAUSE VIEW stopped_actions_memory;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name SETTINGS max_block_size = 1;

-- Predicates on the new column must still materialize it, even when it is not selected.
SELECT name FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' AND has(stopped_actions, 'merges') ORDER BY name;
SYSTEM START MERGES stopped_actions_a;
SYSTEM START MERGES stopped_actions_a;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_a';
SYSTEM START TTL MERGES stopped_actions_a;
SYSTEM START MOVES stopped_actions_a;
SYSTEM START CLEANUP stopped_actions_a;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name;

SYSTEM STOP MERGES stopped_actions_a;
RENAME TABLE stopped_actions_a TO stopped_actions_renamed;
CREATE TABLE stopped_actions_a (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name IN ('stopped_actions_a', 'stopped_actions_renamed') ORDER BY name;
SYSTEM START MERGES stopped_actions_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';

-- The new storage instance must not inherit expired locks, even before another control query cleans them up.
SYSTEM STOP MERGES stopped_actions_renamed;
SYSTEM STOP TTL MERGES stopped_actions_renamed;
DETACH TABLE stopped_actions_renamed SYNC;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';
ATTACH TABLE stopped_actions_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';
SYSTEM STOP MOVES stopped_actions_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_renamed';

-- Temporary rows have a separate materialization path. Include adjacent columns to check alignment.
CREATE TEMPORARY TABLE stopped_actions_temporary (x UInt64);
SELECT name, is_temporary, definer, stopped_actions FROM system.tables
WHERE is_temporary AND name = 'stopped_actions_temporary';
DROP TEMPORARY TABLE stopped_actions_temporary;

DROP TABLE stopped_actions_a SYNC;
DROP TABLE stopped_actions_b SYNC;
DROP TABLE stopped_actions_memory SYNC;
DROP TABLE stopped_actions_renamed SYNC;

CREATE TABLE stopped_actions_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_distributed AS stopped_actions_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), stopped_actions_local);

SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name IN ('stopped_actions_local', 'stopped_actions_distributed') ORDER BY name;
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_distributed;
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_distributed;
-- Unsupported actions must not appear, and controlling a distributed table does not control its target.
SYSTEM STOP MERGES stopped_actions_distributed;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name IN ('stopped_actions_local', 'stopped_actions_distributed') ORDER BY name;
RENAME TABLE stopped_actions_distributed TO stopped_actions_distributed_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_distributed_renamed';
SYSTEM START DISTRIBUTED SENDS stopped_actions_distributed_renamed;
SYSTEM START DISTRIBUTED SENDS stopped_actions_distributed_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_distributed_renamed';
SYSTEM STOP DISTRIBUTED SENDS stopped_actions_distributed_renamed;
DETACH TABLE stopped_actions_distributed_renamed SYNC;
ATTACH TABLE stopped_actions_distributed_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_distributed_renamed';

DROP TABLE stopped_actions_distributed_renamed SYNC;
DROP TABLE stopped_actions_local SYNC;

CREATE TABLE stopped_actions_source (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW stopped_actions_refresh_to REFRESH AFTER 1 YEAR TO stopped_actions_target EMPTY AS
SELECT x FROM stopped_actions_source;
CREATE MATERIALIZED VIEW stopped_actions_refresh_inner REFRESH AFTER 1 YEAR ENGINE = MergeTree ORDER BY x EMPTY AS
SELECT x FROM stopped_actions_source;
CREATE MATERIALIZED VIEW stopped_actions_plain_to TO stopped_actions_target AS SELECT x FROM stopped_actions_source;
CREATE MATERIALIZED VIEW stopped_actions_plain_inner ENGINE = MergeTree ORDER BY x AS SELECT x FROM stopped_actions_source;

SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_%' ORDER BY name;

-- `STOP` and `PAUSE` are distinct explicit controls; `START VIEW` removes both.
SYSTEM STOP VIEW stopped_actions_refresh_to;
SYSTEM STOP VIEW stopped_actions_refresh_to;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_to';
SYSTEM PAUSE VIEW stopped_actions_refresh_to;
SYSTEM PAUSE VIEW stopped_actions_refresh_to;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_to';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_target';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_inner';
SYSTEM START VIEW stopped_actions_refresh_to;
SYSTEM START VIEW stopped_actions_refresh_to;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_to';

-- Reverse the order on a view with an implicit target; also cover the generic control syntax.
SYSTEM PAUSE stopped_actions_refresh_inner;
SYSTEM PAUSE stopped_actions_refresh_inner;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_inner';
SYSTEM STOP stopped_actions_refresh_inner;
SYSTEM STOP stopped_actions_refresh_inner;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_inner';
SELECT stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name =
    (SELECT target_table FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_inner');
SYSTEM START stopped_actions_refresh_inner;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_inner';

SYSTEM PAUSE VIEW stopped_actions_refresh_to;
SYSTEM START VIEW stopped_actions_refresh_to;
SYSTEM STOP VIEW stopped_actions_refresh_inner;
SYSTEM START VIEW stopped_actions_refresh_inner;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_refresh_%' ORDER BY name;

-- Ordinary materialized views have no refresher and must not acquire refresh control locks.
SYSTEM STOP VIEW stopped_actions_plain_to;
SYSTEM PAUSE VIEW stopped_actions_plain_to;
SYSTEM STOP VIEW stopped_actions_plain_inner;
SYSTEM PAUSE VIEW stopped_actions_plain_inner;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_plain_%' ORDER BY name;

-- Existing merge control forwarding to an implicit target is unchanged.
SYSTEM STOP MERGES stopped_actions_plain_inner;
SYSTEM STOP MERGES stopped_actions_plain_to;
SELECT name, stopped_actions FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'stopped_actions_plain_%' ORDER BY name;
SYSTEM START MERGES stopped_actions_plain_inner;

SYSTEM STOP VIEW stopped_actions_refresh_to;
RENAME TABLE stopped_actions_refresh_to TO stopped_actions_refresh_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_renamed';
SYSTEM START VIEW stopped_actions_refresh_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_renamed';
SYSTEM PAUSE VIEW stopped_actions_refresh_renamed;
DETACH TABLE stopped_actions_refresh_renamed SYNC;
ATTACH TABLE stopped_actions_refresh_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_renamed';

-- Cancelling a refresh is not a persistent stop or pause control.
SYSTEM CANCEL VIEW stopped_actions_refresh_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refresh_renamed';

DROP TABLE stopped_actions_refresh_renamed SYNC;
DROP TABLE stopped_actions_refresh_inner SYNC;
DROP TABLE stopped_actions_plain_to SYNC;
DROP TABLE stopped_actions_plain_inner SYNC;
DROP TABLE stopped_actions_target SYNC;
DROP TABLE stopped_actions_source SYNC;
