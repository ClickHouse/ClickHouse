#pragma once

#include <Storages/PostgreSQL/MaterializedPostgreSQLConsumer.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/PostgreSQL/Utils.h>
#include <Parsers/ASTCreateQuery.h>

#include <optional>
#include <unordered_set>


namespace DB
{

struct MaterializedPostgreSQLSettings;
class StorageMaterializedPostgreSQL;
struct SettingChange;

/// True when the engine definition comes from a query the user has just supplied - a `CREATE`, or an
/// `ATTACH` that carries a full engine definition - as opposed to a replay of a definition that was
/// already validated when the object was created: server startup, `RESTORE`, a `Replicated` database's
/// secondary query, or a short-syntax `ATTACH` that reuses the stored definition. Only a fresh definition
/// may be rejected for a setting combination that was previously accepted, so that an existing deployment
/// keeps starting up after an upgrade while a new one fails closed.
bool isFreshEngineDefinition(LoadingStrictnessLevel mode, bool attach_short_syntax);

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

    /// For the database engine on attach: the tables the engine already replicated in the previous run
    /// (their nested tables exist on disk). The attach-time legacy-identity ownership check compares the
    /// legacy publication against this set - not against the live PostgreSQL schema, which may contain
    /// tables created after `CREATE DATABASE` that are not replicated without an explicit `ATTACH TABLE`.
    /// Must be called before fetchRequiredTables() / startup() when attaching a database.
    void setTablesReplicatedByPreviousRun(std::set<String> tables);

    /// Start replication setup immediately.
    void startSynchronization(bool throw_on_error);

    ASTPtr getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name);

    /// Returns true if this call added the table to the publication, and false if the publication
    /// already published it. The caller needs the distinction to roll back only what it created:
    /// dropping a pre-existing publication entry would introduce the very publication drift the
    /// attach checks fail closed on.
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

    /// The (schema, table) pairs an existing publication currently publishes, read from
    /// pg_publication_tables. The schema is normalized so the default schema is reported as "public"
    /// (matching getNormalizedSchemaAndTableName()), so the two sets can be compared by exact pair.
    template <typename T>
    std::set<std::pair<String, String>> fetchPublishedTablePairs(T & tx) const;

    /// An existing publication may only be adopted or resumed through when its definition matches what
    /// ClickHouse creates: all four operation types published (pubinsert, pubupdate, pubdelete,
    /// pubtruncate - CREATE PUBLICATION's defaults), no all-tables membership, and, on PostgreSQL 15 and
    /// newer, no row filter, column list, or schema-level membership. A publication with the right table list
    /// but an altered
    /// definition (for example ALTER PUBLICATION ... SET (publish = 'insert')) silently filters the
    /// replication stream, and the filtered-out changes cannot be recovered once the slot advances.
    /// Returns a human-readable reason when the definition does not match, or an empty string when it does.
    String publicationDefinitionConflict(pqxx::nontransaction & tx, const String & name) const;

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

    /// getSchemaAndTableName() with the default schema normalized to "public", so the resulting pair
    /// can be compared directly against pg_publication_tables (see fetchPublishedTablePairs()).
    std::pair<String, String> getNormalizedSchemaAndTableName(const String & table_name) const;

    /// In the single-schema modes (schema_as_a_part_of_table_name == false) the WAL consumer keys relation
    /// messages by the bare table name (MaterializedPostgreSQLConsumer), so a publication that keeps this
    /// engine's own tables but also publishes a foreign-schema table with the same bare name would have that
    /// foreign table's WAL replayed into this engine's ClickHouse table. Given the set of (schema, table)
    /// pairs this engine expects to replicate and the set the publication currently publishes, returns a
    /// comma-separated list of such colliding foreign-schema extras ("schema.table"), or an empty string if
    /// there are none (extras without a bare-name collision are harmless and are tolerated).
    String collidingForeignPublishedTables(
        const std::set<std::pair<String, String>> & expected,
        const std::set<std::pair<String, String>> & published) const;

    /// Whether any of the storages this handler replicates already has its nested table in the catalog, i.e.
    /// whether a previous run of an object with this UUID materialized data that must not be silently
    /// invalidated by a fresh in-place snapshot.
    bool hasNestedStorage() const;

    void assertInitialized() const;

    void execWithRetryAndFaultInjection(postgres::Connection & connection, const std::function<void(pqxx::nontransaction &)> & exec) const;

    LoggerPtr log;

    /// If it is not attach, i.e. a create query, then if publication already exists - always drop it.
    bool is_attach;

    /// Whether this handler was built from a freshly supplied engine definition - a `CREATE`, or an `ATTACH`
    /// that carries a full engine definition - as opposed to a replay of an already stored definition (server
    /// startup, `RESTORE`, a short-syntax `ATTACH`). A full-definition `ATTACH TABLE` is an attach as far as
    /// `is_attach` is concerned, yet it introduces a brand-new object that has never replicated anything.
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

    String schema_list;

    /// Schema can be as a part of table name, i.e. as a clickhouse table it is accessed like db.`schema.table`.
    /// This is possible to allow replicating tables from multiple schemas in the same MaterializedPostgreSQL database engine.
    mutable bool schema_as_a_part_of_table_name = false;

    /// Whether to map PostgreSQL `date`/`timestamp` to `Date32`/`DateTime64` (true) or to `Date`/`DateTime` (false).
    const bool use_extended_date_and_time_types;

    const bool user_managed_slot;
    const String user_provided_snapshot;
    const bool use_unique_replication_consumer_identifier;
    /// Not const: adoptLegacyReplicationIdentityIfNeeded() switches these to the legacy names once, on
    /// attach of a deployment created before the generated names became schema-aware (or, for a unique
    /// replication consumer identifier, before it became salted with the per-server `ServerUUID`). They
    /// are never modified after the replication consumer is created.
    String replication_slot;
    String tmp_replication_slot;
    String publication_name;
    /// The legacy replication slot and publication names this configuration would have used before the
    /// generated names became schema-aware and before the unique replication consumer identifier became
    /// salted with the per-server `ServerUUID`: schema-unaware, and derived from the bare ClickHouse
    /// object UUID when a unique replication consumer identifier is used. Equal to the current names
    /// when nothing was renamed for this configuration (default PostgreSQL schema and no unique
    /// identifier, or a user-managed slot for the slot's part).
    const String legacy_replication_slot;
    const String legacy_publication_name;

    /// The set of PostgreSQL schemas this engine replicates, normalizing the default schema to `"public"`
    /// (as `pg_publication_tables.schemaname` reports it). Computed once from the raw settings, before
    /// `tables_list` is rewritten during startup. Used to decide whether a schema-blind legacy publication
    /// (whose name carries no schema and could belong to another engine) may be adopted on attach.
    const std::unordered_set<String> replicated_schemas;

    /// See setTablesReplicatedByPreviousRun(). Only provided by the database engine on attach; the
    /// single-table engine carries its one table in `tables_list` instead.
    std::optional<std::set<String>> tables_replicated_by_previous_run;

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

    bool replication_handler_initialized = false;

    float fault_injection_probability = 0.;
};

}
