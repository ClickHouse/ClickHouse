#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaConsumer.h"
#include "PostgreSQLReplicaMetadata.h"
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>


namespace DB
{

class StoragePostgreSQLReplica;

class PostgreSQLReplicationHandler
{
public:
    friend class PGReplicaLSN;
    PostgreSQLReplicationHandler(
            const std::string & database_name_,
            const std::string & conn_str_,
            const std::string & metadata_path_,
            std::shared_ptr<Context> context_,
            const size_t max_block_size_,
            const String tables_list = "");

    void startup();

    void shutdown();

    void shutdownFinal();

    void addStorage(const std::string & table_name, StoragePostgreSQLReplica * storage);

    std::unordered_set<std::string> fetchRequiredTables(PostgreSQLConnection::ConnectionPtr connection_);

    PostgreSQLTableStructure fetchTableStructure(std::shared_ptr<pqxx::work> tx, const std::string & table_name);

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;
    using Storages = std::unordered_map<String, StoragePostgreSQLReplica *>;

    bool isPublicationExist(std::shared_ptr<pqxx::work> tx);

    bool isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name);

    void createPublicationIfNeeded(PostgreSQLConnection::ConnectionPtr connection_);

    void createReplicationSlot(NontransactionPtr ntx, std::string & start_lsn, std::string & snapshot_name, bool temporary = false);

    void dropReplicationSlot(NontransactionPtr tx, std::string & slot_name);

    void dropPublication(NontransactionPtr ntx);

    void waitConnectionAndStart();

    void startSynchronization();

    void consumerFunc();

    void loadFromSnapshot(std::string & snapshot_name, Storages & sync_storages);

    std::unordered_set<std::string> fetchTablesFromPublication(PostgreSQLConnection::ConnectionPtr connection_);

    std::string reloadFromSnapshot(NameSet & table_names);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string database_name, connection_str, metadata_path;
    const size_t max_block_size;
    std::string tables_list, replication_slot, publication_name;

    PostgreSQLConnectionPtr connection;
    std::shared_ptr<PostgreSQLReplicaConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task, consumer_task;
    std::atomic<bool> tables_loaded = false, stop_synchronization = false;
    bool new_publication_created = false;

    Storages storages;
    std::unordered_map<String, StoragePtr> nested_storages;
};

}

#endif

