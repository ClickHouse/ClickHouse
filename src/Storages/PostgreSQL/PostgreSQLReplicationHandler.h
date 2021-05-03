#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include "MaterializePostgreSQLConsumer.h"
#include "MaterializePostgreSQLMetadata.h"

#include <Core/PostgreSQL/PostgreSQLConnection.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>


namespace DB
{

/// IDEA: There is ALTER PUBLICATION command to dynamically add and remove tables for replicating (the command is transactional).
///       (Probably, if in a replication stream comes a relation name, which does not currently
///       exist in CH, it can be loaded via snapshot while stream is stopped and then comparing wal positions with
///       current lsn and table start lsn.

class StorageMaterializePostgreSQL;

class PostgreSQLReplicationHandler
{
public:
    PostgreSQLReplicationHandler(
            const String & remote_database_name_,
            const String & current_database_name_,
            const postgres::ConnectionInfo & connection_info_,
            const String & metadata_path_,
            ContextPtr context_,
            const size_t max_block_size_,
            bool allow_minimal_ddl_,
            bool is_materialize_postgresql_database_,
            const String tables_list = "");

    void startup();

    /// Stop replication without cleanup.
    void shutdown();

    /// Clean up replication: remove publication and replication slots.
    void shutdownFinal();

    void addStorage(const std::string & table_name, StorageMaterializePostgreSQL * storage);

    /// Fetch list of tables which are going to be replicated. Used for database engine.
    NameSet fetchRequiredTables(pqxx::connection & connection_);

    void startSynchronization(bool throw_on_error);

private:
    using MaterializedStorages = std::unordered_map<String, StorageMaterializePostgreSQL *>;

    bool isPublicationExist(pqxx::work & tx);

    void createPublicationIfNeeded(pqxx::work & tx, bool create_without_check = false);

    NameSet fetchTablesFromPublication(pqxx::work & tx);

    void dropPublication(pqxx::nontransaction & ntx);

    bool isReplicationSlotExist(pqxx::nontransaction & tx, std::string & slot_name);

    void createReplicationSlot(pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name, bool temporary = false);

    void dropReplicationSlot(pqxx::nontransaction & tx, bool temporary = false);

    void waitConnectionAndStart();

    void consumerFunc();

    StoragePtr loadFromSnapshot(std::string & snapshot_name, const String & table_name, StorageMaterializePostgreSQL * materialized_storage);

    void reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data);

    PostgreSQLTableStructurePtr fetchTableStructure(pqxx::ReplicationTransaction & tx, const std::string & table_name);

    Poco::Logger * log;
    ContextPtr context;

    /// Remote database name.
    const String remote_database_name, current_database_name;

    /// Path for replication metadata.
    const String metadata_path;

    /// Connection string and address for logs.
    postgres::ConnectionInfo connection_info;

    /// max_block_size for replication stream.
    const size_t max_block_size;

    /// Table structure changes are always tracked. By default, table with changed schema will get into a skip list.
    bool allow_minimal_ddl = false;

    /// To distinguish whether current replication handler belongs to a MaterializePostgreSQL database engine or single storage.
    bool is_materialize_postgresql_database;

    /// A coma-separated list of tables, which are going to be replicated for database engine. By default, a whole database is replicated.
    String tables_list;

    String replication_slot, publication_name;

    postgres::ConnectionPtr connection;

    /// Replication consumer. Manages decoding of replication stream and syncing into tables.
    std::shared_ptr<MaterializePostgreSQLConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task, consumer_task;

    std::atomic<bool> stop_synchronization = false;

    /// For database engine there are 2 places where it is checked for publication:
    /// 1. to fetch tables list from already created publication when database is loaded
    /// 2. at replication startup
    bool new_publication_created = false;

    /// MaterializePostgreSQL tables. Used for managing all operations with its internal nested tables.
    MaterializedStorages materialized_storages;
};

}

#endif
