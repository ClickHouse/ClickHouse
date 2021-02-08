#pragma once

#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaMetadata.h"
#include "pqxx/pqxx"

#include <Core/BackgroundSchedulePool.h>
#include <common/logger_useful.h>
#include <Storages/IStorage.h>
#include <Storages/PostgreSQL/insertPostgreSQLValue.h>
#include <DataStreams/OneBlockInputStream.h>


namespace DB
{

class PostgreSQLReplicaConsumer
{
public:
    using Storages = std::unordered_map<String, StoragePtr>;

    PostgreSQLReplicaConsumer(
            std::shared_ptr<Context> context_,
            PostgreSQLConnectionPtr connection_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_,
            const std::string & metadata_path,
            const std::string & start_lsn,
            const size_t max_block_size_,
            Storages storages_);

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

    bool readFromReplicationSlot();
    void syncTables(std::shared_ptr<pqxx::nontransaction> tx, const std::unordered_set<std::string> & tables_to_sync);
    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    void processReplicationMessage(
            const char * replication_message, size_t size, std::unordered_set<std::string> & tables_to_sync);

    struct BufferData
    {
        ExternalResultDescription description;
        MutableColumns columns;
        /// Needed for insertPostgreSQLValue() method to parse array
        std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

        BufferData(const Block block)
        {
            description.init(block);
            columns = description.sample_block.cloneEmptyColumns();
            for (const auto idx : ext::range(0, description.sample_block.columns()))
                if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
                    preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);
        }
    };

    using Buffers = std::unordered_map<String, BufferData>;

    void insertDefaultValue(BufferData & buffer, size_t column_idx);
    void insertValue(BufferData & buffer, const std::string & value, size_t column_idx);
    void readTupleData(BufferData & buffer, const char * message, size_t & pos, PostgreSQLQuery type, bool old_value = false);

    /// Methods to parse replication message data.
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

    PostgreSQLConnectionPtr connection;

    std::string current_lsn, final_lsn;
    const size_t max_block_size;
    std::string table_to_insert;

    BackgroundSchedulePool::TaskHolder wal_reader_task;
    std::atomic<bool> stop_synchronization = false;

    Storages storages;
    Buffers buffers;
};

}

