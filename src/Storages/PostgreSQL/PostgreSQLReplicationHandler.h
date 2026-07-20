#pragma once

#include <Storages/PostgreSQL/MaterializedPostgreSQLConsumer.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/PostgreSQL/Utils.h>
#include <Parsers/ASTCreateQuery.h>

#include <atomic>
#include <memory>


namespace zkutil
{
    class EphemeralNodeHolder;
    using EphemeralNodeHolderPtr = std::shared_ptr<EphemeralNodeHolder>;
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

struct MaterializedPostgreSQLSettings;
class StorageMaterializedPostgreSQL;
struct SettingChange;

/// Describes the engine used for the nested tables created by a MaterializedPostgreSQL engine.
/// By default it is a plain ReplacingMergeTree. When Keeper coordination is configured
/// (materialized_postgresql_keeper_path is set) it can be a Replicated/Shared ReplacingMergeTree,
/// in which case `zookeeper_path` and `replica_name` are already fully macro-expanded and are
/// passed as the first two engine arguments.
struct NestedTableEngineSpec
{
    String engine_name = "ReplacingMergeTree";
    bool replicated = false;
    String zookeeper_path;
    String replica_name;
};

/// Validate the coordination-related settings of a MaterializedPostgreSQL engine at CREATE time.
/// Throws BAD_ARGUMENTS on an unsupported nested engine name, on a replicated/shared nested engine
/// without materialized_postgresql_keeper_path, or on coordination combined with a unique
/// replication consumer identifier (which would give every replica its own slot instead of the
/// single shared slot that coordination relies on). When coordination is enabled it also requires
/// Keeper/ZooKeeper to be configured (`context`), because both the coordination nodes and the nested
/// replicated tables need it; otherwise CREATE would succeed and the database would sit in a
/// permanent background retry loop.
void validateMaterializedPostgreSQLCoordinationSettings(const MaterializedPostgreSQLSettings & settings, ContextPtr context);

class PostgreSQLReplicationHandler : WithContext
{
friend class TemporaryReplicationSlot;

public:
    using ConsumerPtr = std::shared_ptr<MaterializedPostgreSQLConsumer>;

    PostgreSQLReplicationHandler(
            const String & postgres_database_,
            const String & postgres_table_,
            const String & clickhouse_database_,
            const String & clickhouse_uuid_,
            const postgres::ConnectionInfo & connection_info_,
            ContextPtr context_,
            bool is_attach_,
            const MaterializedPostgreSQLSettings & replication_settings,
            bool is_materialized_postgresql_database_);

    /// Activate task to be run from a separate thread: wait until connection is available and call startReplication().
    void startup(bool delayed);

    /// Stop replication without cleanup.
    void shutdown();

    /// Clean up replication: remove publication and replication slots.
    void shutdownFinal();

    /// True when this handler runs in coordinated mode (materialized_postgresql_keeper_path is set).
    bool isCoordinated() const { return coordination_enabled; }

    /// Coordinated teardown that must run BEFORE the caller deletes this replica's local nested tables (from
    /// DatabaseMaterializedPostgreSQL::beforeDropDatabase for the database engine, and from
    /// StorageMaterializedPostgreSQL::dropInnerTableIfAny for the single-table engine). It makes the fail-close
    /// last-replica decision while this replica still owns the data:
    ///   * if this is the last replica, it removes the shared coordination nodes (including the
    ///     snapshot_completed marker) now, so that even if the subsequent nested-table drop (or the process)
    ///     fails, a later recreate on the same keeper path redoes the initial snapshot instead of resuming
    ///     from confirmed_flush_lsn into empty tables;
    ///   * if this is not the last replica, it re-registers /replicas/<name> so this replica keeps counting as
    ///     a live data holder until its nested tables have actually been dropped - the authoritative removal
    ///     then happens in shutdownFinal, AFTER the caller has dropped the nested tables.
    /// A Keeper error propagates (aborting the drop before any table is removed). Every refusable (throwing)
    /// step runs before the handler is stopped, except the last replica's removal of the shared coordination
    /// nodes; if that step fails, the caller must recover the stopped handler (see `isStopped` /
    /// `restartCoordinatedReplicationAfterFailedTeardown`) so a refused drop does not leave the
    /// database/table mounted but never consuming again.
    void coordinatedTeardownBeforeDataDrop();

    /// Whether `shutdown` has been called (the background tasks are deactivated and the consumer destroyed).
    bool isStopped() const { return stop_synchronization; }

    /// Recovery for a refused (thrown) coordinatedTeardownBeforeDataDrop that had already stopped this
    /// handler: re-arm the retrying background startup path (as on attach), so replication rebuilds itself -
    /// re-registering the replica and re-entering leader election - once Keeper/PostgreSQL are reachable
    /// again, instead of leaving the table mounted but dead until a server restart. Used by the single-table
    /// engine; the database engine instead discards the stopped handler and rebuilds one from scratch.
    void restartCoordinatedReplicationAfterFailedTeardown();

    /// Add storage pointer to let handler know which tables it needs to keep in sync.
    void addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage);

    /// Fetch list of tables which are going to be replicated. Used for database engine.
    std::set<String> fetchRequiredTables();

    /// Start replication setup immediately.
    void startSynchronization(bool throw_on_error);

    ASTPtr getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name);

    void addTableToReplication(StorageMaterializedPostgreSQL * storage, const String & postgres_table_name);

    void removeTableFromReplication(const String & postgres_table_name);

    void setSetting(const SettingChange & setting);

    Strings getTableAllowedColumns(const std::string & table_name) const;

    void cleanupFunc();

private:
    using MaterializedStorages = std::unordered_map<String, StorageMaterializedPostgreSQL *>;

    /// Methods to manage Publication.

    bool isPublicationExist(pqxx::nontransaction & tx);

    void createPublicationIfNeeded(pqxx::nontransaction & tx);

    std::set<String> fetchTablesFromPublication(pqxx::work & tx);

    void dropPublication(pqxx::nontransaction & ntx);

    void addTableToPublication(pqxx::nontransaction & ntx, const String & table_name);

    void removeTableFromPublication(pqxx::nontransaction & ntx, const String & table_name);

    /// Methods to manage Replication Slots.

    bool isReplicationSlotExist(pqxx::nontransaction & tx, String & start_lsn, bool temporary = false);

    void createReplicationSlot(pqxx::nontransaction & tx, String & start_lsn, String & snapshot_name, bool temporary = false);

    void dropReplicationSlot(pqxx::nontransaction & tx, bool temporary = false);

    /// Methods to manage replication.

    void adoptLegacyReplicationIdentityIfNeeded(pqxx::nontransaction & tx);

    void checkConnectionAndStart();

    void consumerFunc();

    /// Build the nested-table engine spec for a given (PostgreSQL) table name, applying the
    /// coordination settings (macros are already expanded once, in the constructor).
    NestedTableEngineSpec makeNestedEngineSpec(const String & table_name) const;

    /// Create the nested tables on every replica (as replicas of the shared replicated tree) without
    /// loading the initial snapshot. Used when coordination is enabled: only the active worker loads
    /// the snapshot, and it propagates to the standbys through ClickHouse replication.
    void ensureNestedTablesExist();

    /// Clear the (already existing) nested tables before redoing the initial snapshot after a
    /// mid-snapshot failover. Only used when coordination is enabled. The nested tables are
    /// Replicated/SharedReplacingMergeTree, so the TRUNCATE propagates to every replica.
    void truncateNestedTables();

    /// Background task (only used when coordination is enabled): try to acquire/keep the ephemeral
    /// leader node in Keeper and start synchronization once leadership is held; otherwise stay a
    /// standby and watch for the leader to disappear.
    void coordinationFunc();

    /// True when coordination is enabled and this handler currently holds the leader node with a live
    /// Keeper session.
    bool isLeader() const;

    /// Register this replica under <keeper_path>/replicas and then create its local nested tables, undoing the
    /// registration if nested-table creation fails. Register-first so that a data-bearing replica is always
    /// registered (see the implementation for the reasoning). Both steps are idempotent.
    void registerReplicaThenEnsureNestedTables();

    /// Register this replica under <keeper_path>/replicas, so that dropping the engine on another
    /// replica knows the shared PostgreSQL objects (slot, publication) are still in use. Idempotent.
    void registerReplicaInKeeper();

    /// Best-effort removal of this replica's <keeper_path>/replicas/<name> node, with no last-replica decision
    /// and without touching any shared state. Used to undo `registerReplicaInKeeper` on a startup error path.
    void unregisterReplica();

    /// True if any of this replica's nested tables has already been created (owns a copy of the shared
    /// replicated data). Used on the register-first error path to decide whether the registration may be undone.
    bool hasAnyNestedTable() const;

    /// Unregister this replica from <keeper_path>/replicas. Returns true when it was the last registered
    /// replica: only then may the caller drop the shared PostgreSQL objects and the coordination nodes. The
    /// last-replica decision is fenced on the /replicas parent node (removing the empty parent succeeds for
    /// exactly one caller), so it is race-free across replicas dropping concurrently.
    bool unregisterReplicaAndCheckLast();

    /// Remove the coordination-owned Keeper nodes (leader, replicas, snapshot marker). Does not touch
    /// <keeper_path>/tables: the nested replicated tables remove their own trees when they are dropped.
    void removeCoordinationNodes();

    /// The durable "initial snapshot finished" marker. An existing replication slot alone does not
    /// prove that the previous active worker finished copying the pre-slot table contents: it may have
    /// died mid-snapshot, and WAL replay from the slot would then permanently miss the rows it never
    /// copied. A new leader may resume from the slot's confirmed LSN only when this marker exists;
    /// otherwise it has to redo the snapshot from scratch.
    bool hasSurvivingCoordinationState();
    bool isInitialSnapshotCompleted();
    void markInitialSnapshotCompleted(const String & lsn);

    ConsumerPtr getConsumer();

    StorageInfo loadFromSnapshot(postgres::Connection & connection, std::string & snapshot_name, const String & table_name, StorageMaterializedPostgreSQL * materialized_storage);

    template<typename T>
    PostgreSQLTableStructurePtr fetchTableStructure(T & tx, const String & table_name) const;

    String doubleQuoteWithSchema(const String & table_name) const;

    std::pair<String, String> getSchemaAndTableName(const String & table_name) const;

    void assertInitialized() const;

    void execWithRetryAndFaultInjection(postgres::Connection & connection, const std::function<void(pqxx::nontransaction &)> & exec) const;

    LoggerPtr log;

    /// If it is not attach, i.e. a create query, then if publication already exists - always drop it.
    bool is_attach;

    String postgres_database;
    String postgres_schema;
    String current_database_name;

    /// Connection string and address for logs.
    postgres::ConnectionInfo connection_info;

    /// max_block_size for replication stream.
    const size_t max_block_size;

    /// To distinguish whether current replication handler belongs to a MaterializedPostgreSQL database engine or single storage.
    bool is_materialized_postgresql_database;

    /// A coma-separated list of tables, which are going to be replicated for database engine. By default, a whole database is replicated.
    String tables_list;

    String schema_list;

    /// Schema can be as a part of table name, i.e. as a clickhouse table it is accessed like db.`schema.table`.
    /// This is possible to allow replicating tables from multiple schemas in the same MaterializedPostgreSQL database engine.
    mutable bool schema_as_a_part_of_table_name = false;

    /// Whether to map PostgreSQL `date`/`timestamp` to `Date32`/`DateTime64` (true) or to `Date`/`DateTime` (false).
    const bool use_extended_date_and_time_types;

    const bool user_managed_slot;
    const String user_provided_snapshot;
    /// Not const: adoptLegacyReplicationIdentityIfNeeded() switches these to the legacy names once, on
    /// attach of a deployment created before the generated names became schema-aware. They are never
    /// modified after the replication consumer is created.
    String replication_slot;
    String tmp_replication_slot;
    String publication_name;
    /// The legacy, schema-unaware replication slot and publication names this configuration would have
    /// used before the generated names became schema-aware. Equal to the current names when the engine
    /// targets the default PostgreSQL schema, or (for the slot) when the slot name does not depend on
    /// the schema (a user-managed slot or a unique replication consumer identifier).
    const String legacy_replication_slot;
    const String legacy_publication_name;

    /// Replication consumer. Manages decoding of replication stream and syncing into tables.
    ConsumerPtr consumer;

    BackgroundSchedulePoolTaskHolder startup_task;
    BackgroundSchedulePoolTaskHolder consumer_task;
    BackgroundSchedulePoolTaskHolder cleanup_task;
    /// Only activated when coordination is enabled (see coordination_enabled below).
    BackgroundSchedulePoolTaskHolder coordination_task;

    const UInt64 reschedule_backoff_min_ms;
    const UInt64 reschedule_backoff_max_ms;
    const UInt64 reschedule_backoff_factor;
    UInt64 milliseconds_to_wait;

    std::atomic<bool> stop_synchronization = false;

    /// Set by restartCoordinatedReplicationAfterFailedTeardown: makes the background startup task keep
    /// retrying on error even when `is_attach` is false, mirroring the attach behavior (the setup already
    /// exists, so a synchronous failure cannot be reported to any query anyway).
    std::atomic<bool> retry_startup_on_error = false;

    /// MaterializedPostgreSQL tables. Used for managing all operations with its internal nested tables.
    MaterializedStorages materialized_storages;

    bool replication_handler_initialized = false;

    float fault_injection_probability = 0.;

    /// Cross-replica coordination of the (single) PostgreSQL replication slot. Enabled when
    /// `materialized_postgresql_keeper_path` is set. When enabled, exactly one replica (the holder of
    /// `leader_node`) consumes the slot; the others create the nested tables as replicas of the same
    /// shared replicated tree and wait to take over.
    bool coordination_enabled = false;
    /// Fully macro-expanded values (computed once in the constructor).
    String coordination_keeper_path;
    String coordination_replica_name;
    /// Set by coordinatedTeardownBeforeDataDrop (the race-free pre-data last-replica decision) so that the
    /// post-data shutdownFinal does not re-decide when this replica already claimed the last-replica role and
    /// removed the shared /replicas node itself (its absence would otherwise read as "another replica was last").
    bool coordinated_teardown_was_last = false;
    /// One of "ReplacingMergeTree" / "ReplicatedReplacingMergeTree" / "SharedReplacingMergeTree".
    String nested_engine_name;
    /// The ephemeral Keeper node marking this replica as the active worker. Non-null only while leader.
    /// Together with `coordination_zookeeper` (which keeps the referenced session alive), these are
    /// only ever mutated from `coordination_task`, so they need no extra locking. The `/leader` node is
    /// ephemeral, so Keeper removes it automatically once the leader's session ends - a peer then wins
    /// the next `tryCreate`. PostgreSQL's own single-active-session rule on the slot is the ultimate
    /// backstop that prevents two consumers from advancing the slot at once during a handover.
    zkutil::EphemeralNodeHolderPtr leader_node;
    zkutil::ZooKeeperPtr coordination_zookeeper;
    /// Mirror of "this handler currently holds the leader node", read by `consumerFunc` (which runs on a
    /// different task) to decide whether it may consume. Only written by `coordination_task`.
    std::atomic<bool> is_active_worker = false;
};

}
