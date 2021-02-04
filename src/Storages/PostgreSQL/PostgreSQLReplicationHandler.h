#pragma once

#include <common/logger_useful.h>
#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaConsumer.h"
#include "PostgreSQLReplicaMetadata.h"
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include "pqxx/pqxx"


/* Implementation of logical streaming replication protocol: https://www.postgresql.org/docs/10/protocol-logical-replication.html.
 */

namespace DB
{

class PostgreSQLReplicationHandler
{
public:
    friend class PGReplicaLSN;
    PostgreSQLReplicationHandler(
            const std::string & database_name_,
            const std::string & table_name_,
            const std::string & conn_str_,
            std::shared_ptr<Context> context_,
            const std::string & publication_slot_name_,
            const std::string & replication_slot_name_,
            const size_t max_block_size_);

    void startup(StoragePtr storage_);
    void shutdown();
    void shutdownFinal();

private:
    using NontransactionPtr = std::shared_ptr<pqxx::nontransaction>;

    void waitConnectionAndStart();
    bool isPublicationExist();
    void createPublication();

    bool isReplicationSlotExist(NontransactionPtr ntx, std::string & slot_name);
    void createTempReplicationSlot(NontransactionPtr ntx, LSNPosition & start_lsn, std::string & snapshot_name);
    void createReplicationSlot(NontransactionPtr ntx);
    void dropReplicationSlot(NontransactionPtr tx, std::string & slot_name);
    void dropPublication(NontransactionPtr ntx);

    void startReplication();
    void loadFromSnapshot(std::string & snapshot_name);
    Context createQueryContext();
    void getTableOutput(const Context & query_context);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string database_name, table_name;

    std::string publication_name, replication_slot;
    std::string tmp_replication_slot;
    const size_t max_block_size;

    PostgreSQLConnectionPtr connection, replication_connection;
    std::shared_ptr<pqxx::work> tx;

    const String metadata_path;
    BackgroundSchedulePool::TaskHolder startup_task;
    std::shared_ptr<PostgreSQLReplicaConsumer> consumer;
    StoragePtr nested_storage;
};


}

