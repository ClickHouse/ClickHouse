-- Tags: zookeeper, no-replicated-database, no-ordinary-database, need-query-parameters
-- no-replicated-database: the test creates a Replicated database of its own
-- no-ordinary-database: `implicit_transaction` needs an Atomic database

-- `ALTER DATABASE ... MODIFY SETTING` for the replica-local settings of a `Replicated` database.
-- The metadata file is the source of truth for these, so every assertion reads `engine_full`,
-- which `system.databases` renders from the on-disk `CREATE DATABASE` query.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
-- Named collections are server-global, so the name is fixed rather than derived from the test
-- database, and a leftover from an earlier run is dropped first.
DROP NAMED COLLECTION IF EXISTS collection_05055;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/test/' || currentDatabase() || '/alter_local_settings', 'shard1', 'replica1');

SELECT 'created', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- Every mutable replica-local setting can be changed, in a single statement.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING
    max_broken_tables_ratio = 0.5,
    max_replication_lag_to_enqueue = 100,
    wait_entry_commited_timeout_sec = 60,
    check_consistency = 0,
    max_retries_before_automatic_recovery = 3,
    allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views = 1;

SELECT 'altered', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- The new values survive a reload from the metadata file.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'reattached', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- A setting the engine does not have.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING no_such_setting = 1; -- { serverError BAD_ARGUMENTS }

-- The immutable settings. The `default_replica_*` ones are consulted only when the engine
-- arguments are omitted at `CREATE`, and `internal_replication` must match the engines of the
-- tables in the database and be identical on every replica.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING default_replica_path = '/other'; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING default_replica_shard_name = 'other'; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING default_replica_name = 'other'; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING internal_replication = 1; -- { serverError QUERY_NOT_ALLOWED }

-- Values are validated too, not just names: zero is out of the domain of a `NonZeroUInt64`
-- setting, and a `Bool` setting cannot take an arbitrary string.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_replication_lag_to_enqueue = 0; -- { serverError BAD_ARGUMENTS }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING check_consistency = 'not_a_bool'; -- { serverError CANNOT_PARSE_BOOL }

SELECT 'after invalid values', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- A valid change next to a rejected one: the whole statement is refused, so the valid change is
-- not applied either. Once with an immutable name, once with an out-of-domain value.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_broken_tables_ratio = 0.9, internal_replication = 1; -- { serverError QUERY_NOT_ALLOWED }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_broken_tables_ratio = 0.9, max_replication_lag_to_enqueue = 0; -- { serverError BAD_ARGUMENTS }

SELECT 'after rejected multi-change', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- Pointing `collection_name` at a collection that does not exist is refused while preparing the
-- new auth info, before anything is published, so the cluster keeps working.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING collection_name = 'no_such_collection'; -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

SELECT 'cluster after failed collection_name', count()
FROM system.clusters WHERE cluster = {CLICKHOUSE_DATABASE_1:String};

-- A successful change rebuilds the cluster auth info and drops the cached cluster, so the next
-- reader connects with the new credentials. `collection_name` is the only mutable setting that
-- feeds a `Cluster` -- the other one `getClusterImpl` reads, `internal_replication`, is immutable
-- -- which is why the invalidation is gated on it.
CREATE NAMED COLLECTION collection_05055 AS cluster_username = 'alice';
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING collection_name = 'collection_05055';

SELECT 'cluster user', user FROM system.clusters WHERE cluster = {CLICKHOUSE_DATABASE_1:String};

-- Clearing `collection_name` is allowed: it resets the cluster auth info to the default instead of
-- looking up a collection named ''. The default username is `default`, not the empty string.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING collection_name = '';

SELECT 'cluster user cleared', user FROM system.clusters WHERE cluster = {CLICKHOUSE_DATABASE_1:String};

SELECT 'collection_name cleared', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- Nothing on this path is transactional, so the statement is refused inside a transaction unless
-- the documented escape hatch is opened.
SET implicit_transaction = 1;
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_broken_tables_ratio = 0.9; -- { serverError NOT_IMPLEMENTED }
SET throw_on_unsupported_query_inside_transaction = 0;
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_broken_tables_ratio = 0.9;
SET implicit_transaction = 0;
SET throw_on_unsupported_query_inside_transaction = 1;

SELECT 'after transaction', replaceAll(engine_full, currentDatabase(), '{db}')
FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP NAMED COLLECTION collection_05055;
