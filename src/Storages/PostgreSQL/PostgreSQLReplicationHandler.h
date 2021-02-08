#pragma once

#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaConsumer.h"
#include "PostgreSQLReplicaMetadata.h"


namespace DB
{

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
    void addStoragePtr(const std::string & table_name, StoragePtr storage);
    void shutdown();
    void shutdownFinal();

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;
    using Storages = std::unordered_map<String, StoragePtr>;

    bool isPublicationExist(std::shared_ptr<pqxx::work> tx);
    bool isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name);

    void createPublication(std::shared_ptr<pqxx::work> tx);
    void createReplicationSlot(NontransactionPtr ntx, std::string & start_lsn, std::string & snapshot_name);

    void dropReplicationSlot(NontransactionPtr tx, std::string & slot_name);
    void dropPublication(NontransactionPtr ntx);

    void waitConnectionAndStart();
    void startReplication();
    void loadFromSnapshot(std::string & snapshot_name);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string database_name, connection_str, metadata_path;
    std::string publication_name, replication_slot;
    const size_t max_block_size;

    PostgreSQLConnectionPtr connection, replication_connection;
    std::shared_ptr<PostgreSQLReplicaConsumer> consumer;

    BackgroundSchedulePool::TaskHolder startup_task;

    Storages storages;
};


}

