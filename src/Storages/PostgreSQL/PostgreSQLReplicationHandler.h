#pragma once

#include <Storages/PostgreSQL/MaterializedPostgreSQLConsumer.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/PostgreSQL/Utils.h>
#include <Parsers/ASTCreateQuery.h>

#include <map>
#include <set>


namespace DB
{

struct MaterializedPostgreSQLSettings;
class StorageMaterializedPostgreSQL;
struct SettingChange;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

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
            bool is_fresh_definition_,
            const MaterializedPostgreSQLSettings & replication_settings,
            bool is_materialized_postgresql_database_);

    /// Activate task to be run from a separate thread: wait until connection is available and call startReplication().
    void startup(bool delayed);

    /// Stop replication without cleanup.
    void shutdown();

    /// Clean up replication: remove publication and replication slots.
    void shutdownFinal();

    /// Add storage pointer to let handler know which tables it needs to keep in sync.
    void addStorage(const std::string & table_name, StorageMaterializedPostgreSQL * storage);

    /// Fetch list of tables which are going to be replicated. Used for database engine.
    std::set<String> fetchRequiredTables();

    /// Start replication setup immediately.
    void startSynchronization(bool throw_on_error);

    /// Tables passed to addStorage that startSynchronization does not replicate, because on attach they had
    /// neither a nested table nor a publication entry (see startSynchronization).
    const std::set<String> & getTablesSkippedOnAttach() const { return tables_skipped_on_attach; }

    /// The file where the database engine persists the snapshot positions of the tables that take a snapshot while the
    /// shared replication slot is behind it (see `snapshot_positions_path`). Must be called before startSynchronization.
    void setSnapshotPositionsFile(DiskPtr disk, const String & path);

    /// Called by `DETACH TABLE ... PERMANENTLY` before the table is removed from the persisted table list: if the
    /// detach is interrupted after that, the publication entry of the table is removed on the next attach.
    void markPublicationEntryOwned(const String & table_name) const;

    ASTPtr getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name);

    /// Returns true if this call added the table to the publication, and false if the publication already
    /// published it. The caller needs the distinction to roll back only what it created: dropping a
    /// pre-existing publication entry would introduce the publication drift that the attach refuses.
    bool addTableToReplication(StorageMaterializedPostgreSQL * storage, const String & postgres_table_name);

    void removeTableFromReplication(const String & postgres_table_name, bool remove_from_publication);

    void setSetting(const SettingChange & setting);

    Strings getTableAllowedColumns(const std::string & table_name) const;

    void cleanupFunc();

private:
    using MaterializedStorages = std::unordered_map<String, StorageMaterializedPostgreSQL *>;

    /// Methods to manage Publication.

    bool isPublicationExist(pqxx::nontransaction & tx);

    void createPublicationIfNeeded(pqxx::nontransaction & tx);

    std::set<String> fetchTablesFromPublication(pqxx::work & tx);

    /// The (schema, table) pairs the publication currently publishes, with the default schema reported as
    /// "public", so that they can be compared with getNormalizedSchemaAndTableName.
    std::set<std::pair<String, String>> fetchPublishedTablePairs(pqxx::nontransaction & tx) const;

    /// An existing publication may only be resumed through when its definition matches what ClickHouse creates:
    /// all operation types are published, no `FOR ALL TABLES`, and, since PostgreSQL 15, no row filter, no schema-level
    /// membership, and no column list other than the ones `materialized_postgresql_tables_list` requests.
    /// Otherwise the replication stream is silently filtered, and the filtered-out changes cannot be recovered later.
    /// Returns a human-readable reason when the definition does not match, or an empty string when it does.
    String publicationDefinitionConflict(pqxx::nontransaction & tx) const;

    void dropPublication(pqxx::nontransaction & ntx);

    /// Returns true if the table was added, and false if the publication already published it.
    bool addTableToPublication(pqxx::nontransaction & ntx, const String & table_name);

    void removeTableFromPublication(pqxx::nontransaction & ntx, const String & table_name);

    /// Methods to manage Replication Slots.

    bool isReplicationSlotExist(pqxx::nontransaction & tx, String & start_lsn, bool temporary = false);

    void createReplicationSlot(pqxx::nontransaction & tx, String & start_lsn, String & snapshot_name, bool temporary = false);

    void dropReplicationSlot(pqxx::nontransaction & tx, bool temporary = false);

    /// Methods to manage replication.

    void adoptLegacyReplicationIdentityIfNeeded(pqxx::nontransaction & tx);

    void checkConnectionAndStart();

    void consumerFunc();

    ConsumerPtr getConsumer();

    StorageInfo loadFromSnapshot(postgres::Connection & connection, std::string & snapshot_name, const String & table_name, StorageMaterializedPostgreSQL * materialized_storage);

    template<typename T>
    PostgreSQLTableStructurePtr fetchTableStructure(T & tx, const String & table_name) const;

    String doubleQuoteWithSchema(const String & table_name) const;

    std::pair<String, String> getSchemaAndTableName(const String & table_name) const;

    /// getSchemaAndTableName with the default schema normalized to "public", as in `pg_publication_tables`.
    std::pair<String, String> getNormalizedSchemaAndTableName(const String & table_name) const;

    /// In the single-schema modes the replication consumer identifies relations by their bare name, so a published
    /// table from another schema with the same bare name as a replicated table would have its changes applied
    /// to the replicated table. Returns a comma-separated list of such tables, or an empty string.
    String collidingForeignPublishedTables(
        const std::set<std::pair<String, String>> & expected,
        const std::set<std::pair<String, String>> & published) const;

    /// Whether any of the replicated tables already has its nested table, i.e. this replica holds data from
    /// a previous run, which a fresh snapshot into the existing nested tables would make stale.
    bool hasNestedStorage() const;

    /// Checks the existing replication slot and publication on attach. Throws if resuming from them would silently
    /// lose changes. Returns true if the previous run did not materialize any table, so the leftover replication slot
    /// must be dropped and the initial synchronization must run from scratch.
    bool checkReplicationStateOnAttach(pqxx::nontransaction & tx);

    /// See `snapshot_positions_path`.
    struct SnapshotPosition
    {
        String start_lsn;
        /// False while the snapshot is being loaded. A nested table whose snapshot did not complete (the server
        /// was terminated while loading it) holds only a part of the rows, so it is dropped and loaded again.
        bool completed = false;
        /// True if the publication entry of the table is removed when the table turns out not to be replicated on
        /// attach (an interrupted `ATTACH TABLE` or `DETACH TABLE ... PERMANENTLY`). False if `ATTACH TABLE` reused
        /// a publication entry that existed before it: then it is kept, as the rollback of a failed `ATTACH TABLE` does.
        bool owns_publication_entry = false;
    };
    /// `table_name` -> its snapshot position.
    using SnapshotPositions = std::map<String, SnapshotPosition>;
    SnapshotPositions loadSnapshotPositions() const;
    void saveSnapshotPositions(const SnapshotPositions & positions) const;
    void markSnapshotCompleted(const String & table_name) const;

    /// Drops the nested table of a table whose snapshot did not complete.
    void dropNestedTable(StorageMaterializedPostgreSQL * materialized_storage) const;

    void assertInitialized() const;

    void execWithRetryAndFaultInjection(postgres::Connection & connection, const std::function<void(pqxx::nontransaction &)> & exec) const;

    LoggerPtr log;

    /// If it is not attach, i.e. a create query, then if publication already exists - always drop it.
    bool is_attach;

    /// Whether the engine definition is supplied by the user now (a `CREATE`, or an `ATTACH` with a full definition),
    /// as opposed to a replay of the stored definition of an object that already completed its initial synchronization.
    const bool is_fresh_definition;

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

    /// Whether the set of replicated tables is configured explicitly by `tables_list`: always for the single-table
    /// engine, and for the database engine with `materialized_postgresql_tables_list`. Otherwise, the database engine
    /// replicates whole schemas, and its replicated tables are the ones that have nested tables.
    const bool has_configured_tables_list;

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

    /// The column subsets `materialized_postgresql_tables_list` requests (`table(col1, col2)`), by (schema, table)
    /// with the default schema normalized to "public". The publication is created with exactly these column lists.
    std::map<std::pair<String, String>, std::set<String>> configured_column_lists;

    /// Replication consumer. Manages decoding of replication stream and syncing into tables.
    ConsumerPtr consumer;

    BackgroundSchedulePoolTaskHolder startup_task;
    BackgroundSchedulePoolTaskHolder consumer_task;
    BackgroundSchedulePoolTaskHolder cleanup_task;

    const UInt64 reschedule_backoff_min_ms;
    const UInt64 reschedule_backoff_max_ms;
    const UInt64 reschedule_backoff_factor;
    UInt64 milliseconds_to_wait;

    std::atomic<bool> stop_synchronization = false;

    /// MaterializedPostgreSQL tables. Used for managing all operations with its internal nested tables.
    MaterializedStorages materialized_storages;

    std::set<String> tables_skipped_on_attach;

    /// A table that takes its snapshot from a temporary replication slot (`ATTACH TABLE`, or the first snapshot of a table
    /// on attach) must not get the changes before the snapshot position applied, but the shared replication slot may be
    /// behind it. The consumer skips them until it reaches that position, and the position is persisted here, so that
    /// it is not lost if the server restarts before the shared replication slot passes it. The entry is written before
    /// the snapshot is loaded and marked completed after it, so that a nested table left by an interrupted snapshot is
    /// loaded again on attach rather than resumed. A completed entry is removed on attach once the shared replication
    /// slot has passed it. Not set for the single-table engine: its replication slot is moved to the snapshot position
    /// instead, because it does not stream changes of other tables.
    DiskPtr snapshot_positions_disk;
    String snapshot_positions_path;

    bool replication_handler_initialized = false;

    float fault_injection_probability = 0.;
};

}
