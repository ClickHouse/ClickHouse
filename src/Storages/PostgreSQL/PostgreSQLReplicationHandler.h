#pragma once

#include "MaterializedPostgreSQLConsumer.h"
#include "MaterializedPostgreSQLSettings.h"
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Core/PostgreSQL/Utils.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{

class StorageMaterializedPostgreSQL;
struct SettingChange;

class PostgreSQLReplicationHandler
{
public:
    PostgreSQLReplicationHandler(
            const String & replication_identifier,
            const String & remote_database_name_,
            const String & current_database_name_,
            const postgres::ConnectionInfo & connection_info_,
            ContextPtr context_,
            bool is_attach_,
            const MaterializedPostgreSQLSettings & replication_settings,
            bool is_materialized_postgresql_database_);

    /// Activate task to be run from a separate thread: wait until connection is available and call startReplication().
    void startup();

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

    ASTPtr getCreateNestedTableQuery(StorageMaterializedPostgreSQL * storage, const String & table_name);

    void addTableToReplication(StorageMaterializedPostgreSQL * storage, const String & postgres_table_name);

    void removeTableFromReplication(const String & postgres_table_name);

    void setSetting(const SettingChange & setting);

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

    void waitConnectionAndStart();

    void consumerFunc();

    StoragePtr loadFromSnapshot(postgres::Connection & connection, std::string & snapshot_name, const String & table_name, StorageMaterializedPostgreSQL * materialized_storage);

    void reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data);

    PostgreSQLTableStructurePtr fetchTableStructure(pqxx::ReplicationTransaction & tx, const String & table_name) const;

    Poco::Logger * log;
    ContextPtr context;

    /// If it is not attach, i.e. a create query, then if publication already exists - always drop it.
    bool is_attach;

    const String remote_database_name, current_database_name;

    /// Connection string and address for logs.
    postgres::ConnectionInfo connection_info;

    /// max_block_size for replication stream.
    const size_t max_block_size;

    /// Table structure changes are always tracked. By default, table with changed schema will get into a skip list.
    /// This setting allows to reloas table in the background.
    bool allow_automatic_update = false;

    /// To distinguish whether current replication handler belongs to a MaterializedPostgreSQL database engine or single storage.
    bool is_materialized_postgresql_database;

    /// A coma-separated list of tables, which are going to be replicated for database engine. By default, a whole database is replicated.
    String tables_list;

    bool user_managed_slot = true;
    String user_provided_snapshot;

    String replication_slot, publication_name;

    /// Replication consumer. Manages decoding of replication stream and syncing into tables.
    std::shared_ptr<MaterializedPostgreSQLConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task, consumer_task;

    std::atomic<bool> stop_synchronization = false;

    /// MaterializedPostgreSQL tables. Used for managing all operations with its internal nested tables.
    MaterializedStorages materialized_storages;

    UInt64 milliseconds_to_wait;

    String postgres_schema;
};

}
