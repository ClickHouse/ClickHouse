#pragma once

#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaMetadata.h"
#include "pqxx/pqxx"

#include <Core/BackgroundSchedulePool.h>
#include <common/logger_useful.h>
#include <Storages/IStorage.h>
#include <Storages/PostgreSQL/insertPostgreSQLValue.h>


namespace DB
{

class PostgreSQLReplicaConsumer
{
public:
    PostgreSQLReplicaConsumer(
            std::shared_ptr<Context> context_,
            const std::string & table_name_,
            PostgreSQLConnectionPtr connection_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_,
            const std::string & metadata_path,
            const std::string & start_lsn,
            const size_t max_block_size_,
            StoragePtr nested_storage_);

    /// Start reading WAL from current_lsn position. Initial data sync from created snapshot already done.
    void startSynchronization();
    void stopSynchronization();

private:
    /// Executed by wal_reader_task. A separate thread reads wal and advances lsn to last commited position
    /// after rows were written via copyData.
    void replicationStream();
    void stopReplicationStream();

    enum class PostgreSQLQuery
    {
        INSERT,
        UPDATE,
        DELETE
    };

    /// Start changes stream from WAL via copy command (up to max_block_size changes).
    bool readFromReplicationSlot();
    void processReplicationMessage(const char * replication_message, size_t size);

    void insertValue(std::string & value, size_t column_idx);
    //static void insertValueMaterialized(IColumn & column, uint64_t value);
    void insertDefaultValue(size_t column_idx);

    void syncIntoTable(Block & block);
    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    /// Methods to parse replication message data.
    void readTupleData(const char * message, size_t & pos, PostgreSQLQuery type, bool old_value = false);
    void readString(const char * message, size_t & pos, size_t size, String & result);
    Int64 readInt64(const char * message, size_t & pos);
    Int32 readInt32(const char * message, size_t & pos);
    Int16 readInt16(const char * message, size_t & pos);
    Int8 readInt8(const char * message, size_t & pos);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string replication_slot_name;
    const std::string publication_name;
    PostgreSQLReplicaMetadata metadata;

    const std::string table_name;
    PostgreSQLConnectionPtr connection;

    std::string current_lsn, final_lsn;
    BackgroundSchedulePool::TaskHolder wal_reader_task;
    //BackgroundSchedulePool::TaskHolder table_sync_task;
    std::atomic<bool> stop_synchronization = false;

    const size_t max_block_size;
    StoragePtr nested_storage;
    Block sample_block;
    ExternalResultDescription description;
    MutableColumns columns;
    /// Needed for insertPostgreSQLValue() method to parse array
    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

    size_t data_version = 1;
};

}

