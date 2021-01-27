#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <ext/range.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitors.h>
#include <Common/hex.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static const auto wal_reader_reschedule_ms = 500;
static const auto max_thread_work_duration_ms = 60000;
static const auto max_empty_slot_reads = 20;

PostgreSQLReplicaConsumer::PostgreSQLReplicaConsumer(
    std::shared_ptr<Context> context_,
    const std::string & table_name_,
    const std::string & conn_str,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const LSNPosition & start_lsn)
    : log(&Poco::Logger::get("PostgreSQLReaplicaConsumer"))
    , context(context_)
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , table_name(table_name_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
    , current_lsn(start_lsn)
{
    replication_connection = std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", conn_str));

    wal_reader_task = context->getSchedulePool().createTask("PostgreSQLReplicaWALReader", [this]{ WALReaderFunc(); });
    wal_reader_task->deactivate();
}


void PostgreSQLReplicaConsumer::startSynchronization()
{
    //wal_reader_task->activateAndSchedule();
}


void PostgreSQLReplicaConsumer::stopSynchronization()
{
    stop_synchronization.store(true);
    if (wal_reader_task)
        wal_reader_task->deactivate();
}


void PostgreSQLReplicaConsumer::WALReaderFunc()
{
    size_t count_empty_slot_reads = 0;
    auto start_time = std::chrono::steady_clock::now();

    LOG_TRACE(log, "Starting synchronization thread");

    while (!stop_synchronization)
    {
        if (!readFromReplicationSlot() && ++count_empty_slot_reads == max_empty_slot_reads)
        {
            LOG_TRACE(log, "Reschedule synchronization. Replication slot is empty.");
            break;
        }
        else
            count_empty_slot_reads = 0;

        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        if (duration.count() > max_thread_work_duration_ms)
        {
            LOG_TRACE(log, "Reschedule synchronization. Thread work duration limit exceeded.");
            break;
        }
    }

    if (!stop_synchronization)
        wal_reader_task->scheduleAfter(wal_reader_reschedule_ms);
}


void PostgreSQLReplicaConsumer::readString(const char * message, size_t & pos, size_t size, String & result)
{
    assert(size > pos + 2);
    char current = unhex2(message + pos);
    pos += 2;
    while (pos < size && current != '\0')
    {
        result += current;
        current = unhex2(message + pos);
        pos += 2;
    }
}


Int32 PostgreSQLReplicaConsumer::readInt32(const char * message, size_t & pos)
{
    assert(size > pos + 8);
    Int32 result = (UInt32(unhex2(message + pos)) << 24)
                | (UInt32(unhex2(message + pos + 2)) << 16)
                | (UInt32(unhex2(message + pos + 4)) << 8)
                | (UInt32(unhex2(message + pos + 6)));
    pos += 8;
    return result;
}


Int16 PostgreSQLReplicaConsumer::readInt16(const char * message, size_t & pos)
{
    assert(size > pos + 4);
    Int16 result = (UInt32(unhex2(message + pos)) << 8)
                | (UInt32(unhex2(message + pos + 2)));
    pos += 4;
    return result;
}


Int8 PostgreSQLReplicaConsumer::readInt8(const char * message, size_t & pos)
{
    assert(size > pos + 2);
    Int8 result = unhex2(message + pos);
    pos += 2;
    return result;
}


Int64 PostgreSQLReplicaConsumer::readInt64(const char * message, size_t & pos)
{
    assert(size > pos + 16);
    Int64 result = (UInt64(unhex4(message + pos)) << 48)
                | (UInt64(unhex4(message + pos + 4)) << 32)
                | (UInt64(unhex4(message + pos + 8)) << 16)
                | (UInt64(unhex4(message + pos + 12)));
    pos += 16;
    return result;
}


void PostgreSQLReplicaConsumer::readTupleData(const char * message, size_t & pos, size_t /* size */)
{
    Int16 num_columns = readInt16(message, pos);
    /// 'n' means nullable, 'u' means TOASTed value, 't' means text formatted data
    LOG_DEBUG(log, "num_columns {}", num_columns);
    for (int k = 0; k < num_columns; ++k)
    {
        char identifier = readInt8(message, pos);
        Int32 col_len = readInt32(message, pos);
        String result;
        for (int i = 0; i < col_len; ++i)
        {
            result += readInt8(message, pos);
        }
        LOG_DEBUG(log, "identifier {}, col_len {}, result {}", identifier, col_len, result);
    }
    //readString(message, pos, size, result);
}


void PostgreSQLReplicaConsumer::decodeReplicationMessage(const char * replication_message, size_t size)
{
    /// Skip '\x'
    size_t pos = 2;
    char type = readInt8(replication_message, pos);

    LOG_TRACE(log, "TYPE: {}", type);
    switch (type)
    {
        case 'B': // Begin
        {
            Int64 transaction_end_lsn = readInt64(replication_message, pos);
            Int64 transaction_commit_timestamp = readInt64(replication_message, pos);
            LOG_DEBUG(log, "transaction lsn {}, transaction commit timespamp {}",
                    transaction_end_lsn, transaction_commit_timestamp);
            break;
        }
        case 'C': // Commit
        {
            readInt8(replication_message, pos);
            Int64 commit_lsn = readInt64(replication_message, pos);
            Int64 transaction_end_lsn = readInt64(replication_message, pos);
            /// Since postgres epoch
            Int64 transaction_commit_timestamp = readInt64(replication_message, pos);
            LOG_DEBUG(log, "commit lsn {}, transaction lsn {}, transaction commit timestamp {}",
                    commit_lsn, transaction_end_lsn, transaction_commit_timestamp);
            break;
        }
        case 'O': // Origin
            break;
        case 'R': // Relation
        {
            Int32 relation_id = readInt32(replication_message, pos);
            String relation_namespace, relation_name;
            readString(replication_message, pos, size, relation_namespace);
            readString(replication_message, pos, size, relation_name);
            Int8 replica_identity = readInt8(replication_message, pos);
            Int16 num_columns = readInt16(replication_message, pos);

            LOG_DEBUG(log,
                    "Replication message type 'R', relation_id: {}, namespace: {}, relation name {}, replica identity {}, columns number {}",
                    relation_id, relation_namespace, relation_name, replica_identity, num_columns);

            Int8 key;
            Int32 data_type_id, type_modifier;
            for (uint16_t i = 0; i < num_columns; ++i)
            {
                String column_name;
                key = readInt8(replication_message, pos);
                readString(replication_message, pos, size, column_name);
                data_type_id = readInt32(replication_message, pos);
                type_modifier = readInt32(replication_message, pos);
                LOG_DEBUG(log, "Key {}, column name {}, data type id {}, type modifier {}", key, column_name, data_type_id, type_modifier);
            }

            break;
        }
        case 'Y': // Type
            break;
        case 'I': // Insert
        {
            Int32 relation_id = readInt32(replication_message, pos);
            Int8 new_tuple = readInt8(replication_message, pos);
            LOG_DEBUG(log, "relationID {}, newTuple {}", relation_id, new_tuple);
            readTupleData(replication_message, pos, size);
            break;
        }
        case 'U': // Update
            break;
        case 'D': // Delete
            break;
        case 'T': // Truncate
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected byte1 value {} while parsing replication message", type);
    }
}


/// Read binary changes from replication slot via copy command.
bool PostgreSQLReplicaConsumer::readFromReplicationSlot()
{
    bool slot_empty = true;
    try
    {
        auto tx = std::make_unique<pqxx::nontransaction>(*replication_connection->conn());
        /// up_to_lsn is set to NULL, up_to_n_changes is set to max_block_size.
        std::string query_str = fmt::format(
                "select data FROM pg_logical_slot_peek_binary_changes("
                "'{}', NULL, NULL, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, publication_name);
        pqxx::stream_from stream(*tx, pqxx::from_query, std::string_view(query_str));

        while (true)
        {
            const std::vector<pqxx::zview> * row{stream.read_row()};

            if (!row)
            {
                LOG_TRACE(log, "STREAM REPLICATION END");
                stream.complete();
                tx->commit();
                break;
            }

            slot_empty = false;

            for (const auto idx : ext::range(0, row->size()))
            {
                LOG_TRACE(log, "Replication message: {}", (*row)[idx]);
                decodeReplicationMessage((*row)[idx].c_str(), (*row)[idx].size());
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    return !slot_empty;
}

}


