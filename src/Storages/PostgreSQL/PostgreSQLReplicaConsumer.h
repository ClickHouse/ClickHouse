#pragma once

#include "PostgreSQLConnection.h"
#include <Core/BackgroundSchedulePool.h>
#include <common/logger_useful.h>
#include "pqxx/pqxx"

namespace DB
{

struct LSNPosition
{
    std::string lsn;

    uint64_t getValue()
    {
        uint64_t upper_half, lower_half, result;
        std::sscanf(lsn.data(), "%lX/%lX", &upper_half, &lower_half);
        result = (upper_half << 32) + lower_half;
        //LOG_DEBUG(&Poco::Logger::get("LSNParsing"),
        //        "Created replication slot. upper half: {}, lower_half: {}, start lsn: {}",
        //        upper_half, lower_half, result);
        return result;
    }
};


class PostgreSQLReplicaConsumer
{
public:
    PostgreSQLReplicaConsumer(
            std::shared_ptr<Context> context_,
            const std::string & table_name_,
            const std::string & conn_str_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_,
            const LSNPosition & start_lsn);

    /// Start reading WAL from current_lsn position. Initial data sync from created snapshot already done.
    void startSynchronization();
    void stopSynchronization();

private:
    /// Executed by wal_reader_task. A separate thread reads wal and advances lsn when rows were written via copyData.
    void WALReaderFunc();

    /// Start changes stream from WAL via copy command (up to max_block_size changes).
    bool readFromReplicationSlot();
    void decodeReplicationMessage(const char * replication_message, size_t size);

    /// Methods to parse replication message data.
    void readTupleData(const char * message, size_t & pos, size_t size);
    void readString(const char * message, size_t & pos, size_t size, String & result);
    Int64 readInt64(const char * message, size_t & pos);
    Int32 readInt32(const char * message, size_t & pos);
    Int16 readInt16(const char * message, size_t & pos);
    Int8 readInt8(const char * message, size_t & pos);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string replication_slot_name;
    const std::string publication_name;

    const std::string table_name;
    PostgreSQLConnectionPtr connection, replication_connection;

    LSNPosition current_lsn;
    BackgroundSchedulePool::TaskHolder wal_reader_task;
    std::atomic<bool> stop_synchronization = false;
};

}

