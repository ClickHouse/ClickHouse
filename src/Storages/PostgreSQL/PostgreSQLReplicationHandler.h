#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"
#include "MaterializePostgreSQLConsumer.h"
#include "MaterializePostgreSQLMetadata.h"
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
            const std::string & conn_str_,
            const std::string & metadata_path_,
            const Context & context_,
            const size_t max_block_size_,
            bool allow_minimal_ddl_,
            bool is_postgresql_replica_database_engine_,
            const String tables_list = "");

    void startup();

    void shutdown();

    void shutdownFinal();

    void addStorage(const std::string & table_name, StorageMaterializePostgreSQL * storage);

    NameSet fetchRequiredTables(pqxx::connection & connection_);

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;
    using Storages = std::unordered_map<String, StorageMaterializePostgreSQL *>;

    bool isPublicationExist(std::shared_ptr<pqxx::work> tx);

    bool isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name);

    void createPublicationIfNeeded(pqxx::connection & connection_);

    void createReplicationSlot(NontransactionPtr ntx, std::string & start_lsn, std::string & snapshot_name, bool temporary = false);

    void dropReplicationSlot(NontransactionPtr tx, bool temporary = false);

    void dropPublication(NontransactionPtr ntx);

    void waitConnectionAndStart();

    void startSynchronization();

    void consumerFunc();

    NameSet loadFromSnapshot(std::string & snapshot_name, Storages & sync_storages);

    NameSet fetchTablesFromPublication(pqxx::connection & connection_);

    std::unordered_map<Int32, String> reloadFromSnapshot(const std::vector<std::pair<Int32, String>> & relation_data);

    PostgreSQLTableStructurePtr fetchTableStructure(std::shared_ptr<pqxx::ReplicationTransaction> tx, const std::string & table_name);

    Poco::Logger * log;
    const Context & context;
    const std::string database_name, connection_str, metadata_path;
    const size_t max_block_size;
    bool allow_minimal_ddl, is_postgresql_replica_database_engine;
    std::string tables_list, replication_slot, publication_name;

    postgres::ConnectionPtr connection;
    std::shared_ptr<MaterializePostgreSQLConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task, consumer_task;
    std::atomic<bool> tables_loaded = false, stop_synchronization = false;
    bool new_publication_created = false;

    Storages storages;
    std::unordered_map<String, StoragePtr> nested_storages;
};

}

#endif
