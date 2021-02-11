#pragma once

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
            const std::string & publication_slot_name_,
            const std::string & replication_slot_name_,
            const size_t max_block_size_);

    void startup();

    void shutdown();

    void shutdownFinal();

    void addStorage(const std::string & table_name, StoragePostgreSQLReplica * storage);

    std::unordered_set<std::string> fetchRequiredTables(PostgreSQLConnection::ConnectionPtr connection_);

    PostgreSQLTableStructure fetchTableStructure(std::shared_ptr<pqxx::work> tx, const std::string & table_name);

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;

    bool isPublicationExist(std::shared_ptr<pqxx::work> tx);
    bool isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name);

    void createPublication(std::shared_ptr<pqxx::work> tx);
    void createReplicationSlot(NontransactionPtr ntx, std::string & start_lsn, std::string & snapshot_name);

    void dropReplicationSlot(NontransactionPtr tx, std::string & slot_name);
    void dropPublication(NontransactionPtr ntx);

    void waitConnectionAndStart();

    void startSynchronization();

    void loadFromSnapshot(std::string & snapshot_name);

    std::unordered_set<std::string> fetchTablesFromPublication(PostgreSQLConnection::ConnectionPtr connection_);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string database_name, connection_str, metadata_path;
    std::string publication_name, replication_slot;
    const size_t max_block_size;

    PostgreSQLConnectionPtr connection, replication_connection;
    std::shared_ptr<PostgreSQLReplicaConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task;
    std::atomic<bool> tables_loaded = false;

    std::unordered_map<String, StoragePostgreSQLReplica *> storages;
    std::unordered_map<String, StoragePtr> nested_storages;
};


}

