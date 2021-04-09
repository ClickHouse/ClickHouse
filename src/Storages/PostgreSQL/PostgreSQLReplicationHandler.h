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
            const std::string & database_name_,
            const postgres::ConnectionInfo & connection_info_,
            const std::string & metadata_path_,
            const Context & context_,
            const size_t max_block_size_,
            bool allow_minimal_ddl_,
            bool is_postgresql_replica_database_engine_,
            const String tables_list = "");

    void startup();

    /// Stop replication without cleanup.
    void shutdown();

    /// Clean up replication: remove publication and replication slots.
    void shutdownFinal();

    void addStorage(const std::string & table_name, StorageMaterializePostgreSQL * storage);

    /// Fetch list of tables which are going to be replicated. Used for database engine.
    NameSet fetchRequiredTables(pqxx::connection & connection_);

private:
    using Storages = std::unordered_map<String, StorageMaterializePostgreSQL *>;

    void createPublicationIfNeeded(pqxx::connection & connection_);

    bool isPublicationExist(pqxx::work & tx);

    bool isReplicationSlotExist(pqxx::nontransaction & tx, std::string & slot_name);

    void createReplicationSlot(pqxx::nontransaction & tx, std::string & start_lsn, std::string & snapshot_name, bool temporary = false);

    void dropReplicationSlot(pqxx::nontransaction & tx, bool temporary = false);

    void dropPublication(pqxx::nontransaction & ntx);

    void waitConnectionAndStart();

    void startSynchronization();

    void consumerFunc();

    NameSet loadFromSnapshot(std::string & snapshot_name, Storages & sync_storages);

    NameSet fetchTablesFromPublication(pqxx::connection & connection_);

    std::unordered_map<Int32, String> reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data);

    PostgreSQLTableStructurePtr fetchTableStructure(std::shared_ptr<pqxx::ReplicationTransaction> tx, const std::string & table_name);

    Poco::Logger * log;
    const Context & context;

    /// Remote database name.
    const String database_name;

    /// Path for replication metadata.
    const String metadata_path;

    /// Connection string and address for logs.
    postgres::ConnectionInfo connection_info;

    /// max_block_size for replication stream.
    const size_t max_block_size;

    /// Table structure changes are always tracked. By default, table with changed schema will get into a skip list.
    bool allow_minimal_ddl = false;

    /// To distinguish whether current replication handler belongs to a MaterializePostgreSQL database engine or single storage.
    bool is_postgresql_replica_database_engine;

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
    Storages storages;

    /// List of nested tables, which is passed to replication consumer.
    std::unordered_map<String, StoragePtr> nested_storages;
};

}

#endif
