#pragma once

#include <Storages/PostgreSQL/MaterializedPostgreSQLConsumer.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/PostgreSQL/Utils.h>
#include <Parsers/ASTCreateQuery.h>

#include <atomic>
#include <memory>
#include <mutex>


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
/// permanent background retry loop. `clickhouse_database_name` and `clickhouse_uuid` identify the
/// database (or single table) being created: the coordination macros are expanded with them, exactly
/// as the replication handler will expand them later, so an invalid macro fails at CREATE time.
/// When the coordinated setup already exists in Keeper, the naming-affecting settings of this replica
/// are also checked against it, so a replica that would derive different ClickHouse table names from
/// the shared publication is rejected synchronously. `postgres_database` and `postgres_table` carry the
/// PostgreSQL source identity of the engine being created (the table name is empty for the database
/// engine); they are part of that check, because the names of the shared replication slot and
/// publication are derived from them - a setup replicating a different source must not join the same
/// keeper path even when every ClickHouse-side setting matches.
/// `allow_uuid_macro` tells whether the `{uuid}` macro is known to expand to the same value on every
/// replica - that is only true when the UUID is not generated independently per server: an
/// `ON CLUSTER` (or `Replicated` database) DDL, or an explicit `UUID '...'` clause. When it is false,
/// a coordinated keeper path that depends on `{uuid}` is rejected, because each replica would land on
/// a disjoint Keeper subtree while still contending for the same PostgreSQL slot and publication.
void validateMaterializedPostgreSQLCoordinationSettings(
    const MaterializedPostgreSQLSettings & settings,
    ContextPtr context,
    const String & clickhouse_database_name,
    const UUID & clickhouse_uuid,
    const String & postgres_database,
    const String & postgres_table,
    bool allow_uuid_macro);

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

    /// Quiesces the background tasks before any other member is destroyed. The task functions capture `this`
    /// and use members that are declared after the task holders (and are therefore destroyed before them), so
    /// relying on the holders' own destructors would let a still-running task read freed memory. This matters
    /// because a handler can be discarded without a preceding `shutdown`: the retrying background startup path
    /// of the database engine replaces a handler that never managed to start
    /// (DatabaseMaterializedPostgreSQL::startSynchronization), and a refused drop discards a stopped one
    /// (DatabaseMaterializedPostgreSQL::recoverAfterRefusedDrop).
    ~PostgreSQLReplicationHandler();

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
    ///   * if this is not the last replica, its /replicas/<name> registration stays untouched (the decision is
    ///     one atomic Keeper multi-request that removes the registration only together with winning the
    ///     last-replica fence), so even across a process death this replica keeps counting as a live data
    ///     holder until its nested tables have actually been dropped - the authoritative removal then happens
    ///     in shutdownFinal, AFTER the caller has dropped the nested tables.
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

    /// Recovery for the plain (non-coordinated) single-table engine's refused DROP TABLE: the handler was
    /// stopped by `flushAndShutdown`, but the nested table and the PostgreSQL slot/publication survived
    /// (the nested-table drop threw before `shutdownFinal` ran). Re-arm the retrying background startup
    /// path in attach mode, so replication resumes from the existing slot instead of leaving the table
    /// mounted but dead until a server restart.
    void restartReplicationAfterFailedDrop();

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

    /// Check the coordination identity this handler resolved (the macro-expanded
    /// materialized_postgresql_keeper_path and materialized_postgresql_replica_name) against the identity
    /// persisted in the metadata of the nested tables this replica already owns, throwing BAD_ARGUMENTS on a
    /// mismatch. The expansion happens against the *current* server configuration, so a change of the macros
    /// it goes through would silently move this replica's coordination bookkeeping to a different Keeper
    /// identity than the shared data it already holds. Idempotent and a no-op for a replica that has no
    /// nested tables yet.
    void assertCoordinationIdentityMatchesNestedTables() const;

    /// Throw when the coordination path / replica name could not be expanded from the current server
    /// configuration (the constructor records that instead of throwing, to keep a misconfigured setup
    /// droppable). Called by everything that forms a Keeper path from the coordination identity.
    void assertCoordinationIdentityResolved() const;

    /// The coordination identity persisted in a nested table's engine arguments.
    struct PersistedNestedIdentity
    {
        /// Keeper path of the shared replicated tree of this nested table (`keeper_path/tables/table`).
        String zookeeper_path;
        /// Name of this replica in that tree.
        String replica_name;
    };

    /// Read the coordination identity persisted in the metadata of the nested tables this replica owns,
    /// keyed by PostgreSQL table name. Tables whose engine carries no such identity (a plain, unreplicated
    /// nested table) are omitted. Never regenerates the definition from the live settings.
    std::map<String, PersistedNestedIdentity> readPersistedNestedIdentities() const;

    /// Switch the coordination identity of this handler to the one persisted in the nested tables it owns,
    /// so that the teardown deletes the coordination state that actually exists. Only used on the DROP path.
    void adoptPersistedCoordinationIdentityForTeardown();

    /// Remove the <keeper_path>/replicas children that store this replica's own owner identity under a name
    /// other than its current coordination name. Such a node is a leftover of an earlier expansion of
    /// `materialized_postgresql_replica_name` published before the first nested table existed (so the
    /// nested-table metadata cannot catch the change), and it would keep /replicas non-empty forever, making
    /// every future last-replica teardown of the setup decide it is not the last one. Run both before this
    /// replica registers itself and before the last-replica fence on the drop path.
    void purgeOwnStaleReplicaRegistrations();

    /// Publish this replica's naming-affecting settings at <keeper_path>/naming (first replica) or check
    /// them against the already published ones (joining replica), throwing BAD_ARGUMENTS on a mismatch.
    /// All coordinated replicas derive the ClickHouse names of the shared nested tables from the shared
    /// publication through these settings, so replicas that disagree on them would build disjoint
    /// replicated trees on the same keeper path. Also refuses to proceed while the setup is still being
    /// torn down by a last-replica drop (see the <keeper_path>/teardown ownership token). Idempotent.
    void ensureCoordinatedNamingCompatible();

    /// Publish this replica's derived table set at <keeper_path>/table_set (first replica) or check it
    /// against the already published one (joining replica), throwing on a mismatch. This fences the
    /// authoritative shared table set BEFORE any nested table is built: without it, two fresh replicas
    /// starting concurrently (before the shared publication exists) could derive different table sets
    /// (different materialized_postgresql_tables_list values, or the same empty setting around a source
    /// schema change) and silently build diverging nested tables on the same keeper path. Idempotent.
    void ensureCoordinatedTableSetCompatible();

    /// Read the fenced shared table set from <keeper_path>/table_set, or return nullopt if the node does
    /// not exist. Used when the shared publication is (temporarily) absent, so that the authoritative set
    /// is taken from Keeper instead of a stale local materialized_postgresql_tables_list.
    std::optional<std::set<String>> readCoordinatedTableSetFromKeeper();

    /// Register this replica under <keeper_path>/replicas, so that dropping the engine on another
    /// replica knows the shared PostgreSQL objects (slot, publication) are still in use. Idempotent.
    void registerReplicaInKeeper();

    /// Best-effort removal of this replica's <keeper_path>/replicas/<name> node, with no last-replica decision
    /// and without touching any shared state. Used to undo `registerReplicaInKeeper` on a startup error path.
    void unregisterReplica();

    /// Remove this replica's <keeper_path>/replicas/<name> node only if this replica owns it (the node stores
    /// the owning replica's identity). Returns false when the name is held by another replica - the sign of a
    /// duplicate materialized_postgresql_replica_name - whose registration must not be removed. An
    /// already-absent node counts as removed. Throws on Keeper errors.
    bool tryRemoveOwnReplicaRegistration(const zkutil::ZooKeeperPtr & zookeeper);

    /// True if any of this replica's nested tables has already been created (owns a copy of the shared
    /// replicated data). Used on the register-first error path to decide whether the registration may be undone.
    bool hasAnyNestedTable() const;

    /// Decide whether this replica is the last registered one under <keeper_path>/replicas. Returns true when
    /// it was: only then may the caller drop the shared PostgreSQL objects and the coordination nodes. The
    /// last-replica decision is fenced on the /replicas parent node (removing the empty parent succeeds for
    /// exactly one caller), so it is race-free across replicas dropping concurrently; the replica's own
    /// registration is removed atomically with winning the fence. When the replica is NOT the last one,
    /// `keep_registration_when_not_last` selects between keeping its registration untouched (the pre-data
    /// teardown: it is the crash-persistent record that this replica still holds a copy of the shared data)
    /// and removing it (the post-data teardown, once the local nested tables are actually gone).
    bool unregisterReplicaAndCheckLast(bool keep_registration_when_not_last);

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

    /// Throws when the Keeper session backing this worker's /leader node is no longer alive. Fences the
    /// coordinated `startSynchronization` steps that must only be performed by the live active worker
    /// (loading the shared snapshot, publishing the snapshot marker, starting the consumer): a worker that
    /// lost its session mid-snapshot has already been replaced and must abort instead of publishing a
    /// stale marker over its successor's replacement snapshot.
    void assertReplicationLeadershipIsLive() const;

    /// Give up the leadership after a `startSynchronization` attempt failed before a consumer got
    /// running (best effort; called from `coordinationFunc`). The failure may be local to this replica,
    /// and camping on /leader would keep every healthy peer on standby - most importantly for the
    /// single-table engine, whose one failed snapshot load has to abort the whole startup.
    void releaseLeadershipAfterFailedStartup();

    /// Give up the leadership at `shutdown` (graceful stop: `DETACH`, a non-last `DROP`, server shutdown).
    /// Unlike the failed-startup release, nothing re-enters the election afterwards: the leader node lives
    /// under the server's shared Keeper session, so an unconfirmed removal could leave it alive - with no
    /// `removeLeakedOwnLeaderNode` ever running for it - and keep every peer on standby until that shared
    /// session finally closes. So the removal is confirmed here: an ambiguous failure is resolved by
    /// re-checking the node and removing it again (owner- and version-checked) while the removal remains
    /// unconfirmed and the node still provably belongs to this replica's live session.
    void releaseLeadershipAtShutdown();

    /// Remove a <keeper_path>/leader node that this replica created itself and then stopped tracking, which
    /// can only be a leftover of a leadership release whose removal could not be confirmed. Returns true when
    /// the path is free afterwards, so the election can be retried at once. A node held by a live peer, or by
    /// an older Keeper session of this server, is never touched.
    bool removeLeakedOwnLeaderNode(const zkutil::ZooKeeperPtr & zookeeper, const String & leader_path);

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

    /// max_block_size for replication stream. Mutable via `setSetting`, which may run while the consumer
    /// does not exist yet (a coordinated standby, or a plain database whose background startup is still
    /// retrying) - the value is read when the consumer is finally created, hence the atomic.
    std::atomic<size_t> max_block_size;

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
    /// The pointer itself is guarded by `consumer_ptr_mutex` wherever a foreign thread may touch it:
    /// it is assigned by startSynchronization and destroyed by coordinationFunc (leader loss) and
    /// shutdown - all serialized through the background tasks - while setSetting reads it from the
    /// ALTER DATABASE thread. Uses of the pointee via getConsumer stay lock-free: they run inside the
    /// consumer task, which every destroying path deactivates first.
    ConsumerPtr consumer;
    std::mutex consumer_ptr_mutex;

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
    /// Fully macro-expanded values (computed once in the constructor; the DROP path may replace them with the
    /// identity persisted in the nested tables, see `adoptPersistedCoordinationIdentityForTeardown`).
    String coordination_keeper_path;
    /// The naming-affecting settings of this replica, in the canonical form stored at <keeper_path>/naming
    /// (computed once in the constructor; see `ensureCoordinatedNamingCompatible`).
    String coordination_naming_fingerprint;
    String coordination_replica_name;
    /// This replica's identity, stored in its <keeper_path>/replicas/<name> registration node so that a
    /// same-named registration attempt by another replica is rejected instead of silently collapsing two
    /// replicas onto one node (computed once in the constructor; stable across restarts and DETACH/ATTACH).
    String coordination_replica_owner;
    /// Non-empty when the coordination path / replica name could not be expanded from the current server
    /// configuration (a macro they go through was removed after the engine was created). The constructor
    /// records the failure instead of throwing, so that such a setup can still be dropped.
    String coordination_identity_error;
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
