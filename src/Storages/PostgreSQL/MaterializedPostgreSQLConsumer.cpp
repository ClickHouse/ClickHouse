#include "MaterializedPostgreSQLConsumer.h"

#include "StorageMaterializedPostgreSQL.h"
#include <Columns/ColumnNullable.h>
#include <Common/hex.h>
#include <DataStreams/copyData.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MaterializedPostgreSQLConsumer::MaterializedPostgreSQLConsumer(
    ContextPtr context_,
    std::shared_ptr<postgres::Connection> connection_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    const size_t max_block_size_,
    bool allow_automatic_update_,
    Storages storages_)
    : log(&Poco::Logger::get("PostgreSQLReaplicaConsumer"))
    , context(context_)
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , connection(connection_)
    , current_lsn(start_lsn)
    , lsn_value(getLSNValue(start_lsn))
    , max_block_size(max_block_size_)
    , allow_automatic_update(allow_automatic_update_)
    , storages(storages_)
{
    final_lsn = start_lsn;
    auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());
    current_lsn = advanceLSN(tx);
    LOG_TRACE(log, "Starting replication. LSN: {} (last: {})", getLSNValue(current_lsn), getLSNValue(final_lsn));
    tx->commit();

    for (const auto & [table_name, storage] : storages)
    {
        buffers.emplace(table_name, Buffer(storage));
    }
}


void MaterializedPostgreSQLConsumer::Buffer::createEmptyBuffer(StoragePtr storage)
{
    const auto storage_metadata = storage->getInMemoryMetadataPtr();
    const Block sample_block = storage_metadata->getSampleBlock();

    /// Need to clear type, because in description.init() the types are appended (emplace_back)
    description.types.clear();
    description.init(sample_block);

    columns = description.sample_block.cloneEmptyColumns();
    const auto & storage_columns = storage_metadata->getColumns().getAllPhysical();
    auto insert_columns = std::make_shared<ASTExpressionList>();

    auto table_id = storage->getStorageID();
    LOG_TRACE(&Poco::Logger::get("MaterializedPostgreSQLBuffer"), "New buffer for table {}.{} ({}), structure: {}",
              table_id.database_name, table_id.table_name, toString(table_id.uuid), sample_block.dumpStructure());

    assert(description.sample_block.columns() == storage_columns.size());
    size_t idx = 0;

    for (const auto & column : storage_columns)
    {
        if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
            preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);
        idx++;

        insert_columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }

    columnsAST = std::move(insert_columns);
}


void MaterializedPostgreSQLConsumer::insertValue(Buffer & buffer, const std::string & value, size_t column_idx)
{
    const auto & sample = buffer.description.sample_block.getByPosition(column_idx);
    bool is_nullable = buffer.description.types[column_idx].second;

    if (is_nullable)
    {
        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*buffer.columns[column_idx]);
        const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);

        insertPostgreSQLValue(
                column_nullable.getNestedColumn(), value,
                buffer.description.types[column_idx].first, data_type.getNestedType(), buffer.array_info, column_idx);

        column_nullable.getNullMapData().emplace_back(0);
    }
    else
    {
        insertPostgreSQLValue(
                *buffer.columns[column_idx], value,
                buffer.description.types[column_idx].first, sample.type,
                buffer.array_info, column_idx);
    }
}


void MaterializedPostgreSQLConsumer::insertDefaultValue(Buffer & buffer, size_t column_idx)
{
    const auto & sample = buffer.description.sample_block.getByPosition(column_idx);
    insertDefaultPostgreSQLValue(*buffer.columns[column_idx], *sample.column);
}


void MaterializedPostgreSQLConsumer::readString(const char * message, size_t & pos, size_t size, String & result)
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


template<typename T>
T MaterializedPostgreSQLConsumer::unhexN(const char * message, size_t pos, size_t n)
{
    T result = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (i) result <<= 8;
        result |= UInt32(unhex2(message + pos + 2 * i));
    }
    return result;
}


Int64 MaterializedPostgreSQLConsumer::readInt64(const char * message, size_t & pos, [[maybe_unused]] size_t size)
{
    assert(size >= pos + 16);
    Int64 result = unhexN<Int64>(message, pos, 8);
    pos += 16;
    return result;
}


Int32 MaterializedPostgreSQLConsumer::readInt32(const char * message, size_t & pos, [[maybe_unused]] size_t size)
{
    assert(size >= pos + 8);
    Int32 result = unhexN<Int32>(message, pos, 4);
    pos += 8;
    return result;
}


Int16 MaterializedPostgreSQLConsumer::readInt16(const char * message, size_t & pos, [[maybe_unused]] size_t size)
{
    assert(size >= pos + 4);
    Int16 result = unhexN<Int16>(message, pos, 2);
    pos += 4;
    return result;
}


Int8 MaterializedPostgreSQLConsumer::readInt8(const char * message, size_t & pos, [[maybe_unused]] size_t size)
{
    assert(size >= pos + 2);
    Int8 result = unhex2(message + pos);
    pos += 2;
    return result;
}


void MaterializedPostgreSQLConsumer::readTupleData(
        Buffer & buffer, const char * message, size_t & pos, [[maybe_unused]] size_t size, PostgreSQLQuery type, bool old_value)
{
    Int16 num_columns = readInt16(message, pos, size);

    auto proccess_column_value = [&](Int8 identifier, Int16 column_idx)
    {
        switch (identifier)
        {
            case 'n': /// NULL
            {
                insertDefaultValue(buffer, column_idx);
                break;
            }
            case 't': /// Text formatted value
            {
                Int32 col_len = readInt32(message, pos, size);
                String value;

                for (Int32 i = 0; i < col_len; ++i)
                    value += readInt8(message, pos, size);

                insertValue(buffer, value, column_idx);
                break;
            }
            case 'u': /// TOAST value && unchanged at the same time. Actual value is not sent.
            {
                /// TOAST values are not supported. (TOAST values are values that are considered in postgres
                /// to be too large to be stored directly)
                LOG_WARNING(log, "Got TOAST value, which is not supported, default value will be used instead.");
                insertDefaultValue(buffer, column_idx);
                break;
            }
        }
    };

    for (int column_idx = 0; column_idx < num_columns; ++column_idx)
        proccess_column_value(readInt8(message, pos, size), column_idx);

    switch (type)
    {
        case PostgreSQLQuery::INSERT:
        {
            buffer.columns[num_columns]->insert(Int8(1));
            buffer.columns[num_columns + 1]->insert(lsn_value);

            break;
        }
        case PostgreSQLQuery::DELETE:
        {
            buffer.columns[num_columns]->insert(Int8(-1));
            buffer.columns[num_columns + 1]->insert(lsn_value);

            break;
        }
        case PostgreSQLQuery::UPDATE:
        {
            /// Process old value in case changed value is a primary key.
            if (old_value)
                buffer.columns[num_columns]->insert(Int8(-1));
            else
                buffer.columns[num_columns]->insert(Int8(1));

            buffer.columns[num_columns + 1]->insert(lsn_value);

            break;
        }
    }
}


/// https://www.postgresql.org/docs/13/protocol-logicalrep-message-formats.html
void MaterializedPostgreSQLConsumer::processReplicationMessage(const char * replication_message, size_t size)
{
    /// Skip '\x'
    size_t pos = 2;
    char type = readInt8(replication_message, pos, size);
    // LOG_DEBUG(log, "Message type: {}, lsn string: {}, lsn value {}", type, current_lsn, lsn_value);

    switch (type)
    {
        case 'B': // Begin
        {
            readInt64(replication_message, pos, size); /// Int64 transaction end lsn
            readInt64(replication_message, pos, size); /// Int64 transaction commit timestamp
            break;
        }
        case 'I': // Insert
        {
            Int32 relation_id = readInt32(replication_message, pos, size);

            if (!isSyncAllowed(relation_id))
                return;

            Int8 new_tuple = readInt8(replication_message, pos, size);
            const auto & table_name = relation_id_to_name[relation_id];
            auto buffer = buffers.find(table_name);
            assert(buffer != buffers.end());

            if (new_tuple)
                readTupleData(buffer->second, replication_message, pos, size, PostgreSQLQuery::INSERT);

            break;
        }
        case 'U': // Update
        {
            Int32 relation_id = readInt32(replication_message, pos, size);

            if (!isSyncAllowed(relation_id))
                return;

            const auto & table_name = relation_id_to_name[relation_id];
            auto buffer = buffers.find(table_name);
            assert(buffer != buffers.end());

            auto proccess_identifier = [&](Int8 identifier) -> bool
            {
                bool read_next = true;
                switch (identifier)
                {
                    /// Only if changed column(s) are part of replica identity index (or primary keys if they are used instead).
                    /// In this case, first comes a tuple with old replica identity indexes and all other values will come as
                    /// nulls. Then comes a full new row.
                    case 'K': [[fallthrough]];
                    /// Old row. Only if replica identity is set to full. Does not really make sense to use it as
                    /// it is much more efficient to use replica identity index, but support all possible cases.
                    case 'O':
                    {
                        readTupleData(buffer->second, replication_message, pos, size, PostgreSQLQuery::UPDATE, true);
                        break;
                    }
                    case 'N':
                    {
                        /// New row.
                        readTupleData(buffer->second, replication_message, pos, size, PostgreSQLQuery::UPDATE);
                        read_next = false;
                        break;
                    }
                }

                return read_next;
            };

            /// Read either 'K' or 'O'. Never both of them. Also possible not to get both of them.
            bool read_next = proccess_identifier(readInt8(replication_message, pos, size));

            /// 'N'. Always present, but could come in place of 'K' and 'O'.
            if (read_next)
                proccess_identifier(readInt8(replication_message, pos, size));

            break;
        }
        case 'D': // Delete
        {
            Int32 relation_id = readInt32(replication_message, pos, size);

            if (!isSyncAllowed(relation_id))
                return;

             /// 0 or 1 if replica identity is set to full. For now only default replica identity is supported (with primary keys).
            readInt8(replication_message, pos, size);

            const auto & table_name = relation_id_to_name[relation_id];
            auto buffer = buffers.find(table_name);
            assert(buffer != buffers.end());
            readTupleData(buffer->second, replication_message, pos, size, PostgreSQLQuery::DELETE);

            break;
        }
        case 'C': // Commit
        {
            constexpr size_t unused_flags_len = 1;
            constexpr size_t commit_lsn_len = 8;
            constexpr size_t transaction_end_lsn_len = 8;
            constexpr size_t transaction_commit_timestamp_len = 8;
            pos += unused_flags_len + commit_lsn_len + transaction_end_lsn_len + transaction_commit_timestamp_len;

            LOG_DEBUG(log, "Current lsn: {} = {}", current_lsn, getLSNValue(current_lsn)); /// Will be removed

            final_lsn = current_lsn;
            break;
        }
        case 'R': // Relation
        {
            Int32 relation_id = readInt32(replication_message, pos, size);

            String relation_namespace, relation_name;

            readString(replication_message, pos, size, relation_namespace);
            readString(replication_message, pos, size, relation_name);

            if (!isSyncAllowed(relation_id))
                return;

            if (storages.find(relation_name) == storages.end())
            {
                markTableAsSkipped(relation_id, relation_name);
                LOG_ERROR(log,
                          "Storage for table {} does not exist, but is included in replication stream. (Storages number: {})",
                          relation_name, storages.size());
                return;
            }

            assert(buffers.count(relation_name));


            /// 'd' - default (primary key if any)
            /// 'n' - nothing
            /// 'f' - all columns (set replica identity full)
            /// 'i' - user defined index with indisreplident set
            /// Only 'd' and 'i' - are supported.
            char replica_identity = readInt8(replication_message, pos, size);

            if (replica_identity != 'd' && replica_identity != 'i')
            {
                LOG_WARNING(log,
                        "Table has replica identity {} - not supported. A table must have a primary key or a replica identity index");
                markTableAsSkipped(relation_id, relation_name);
                return;
            }

            Int16 num_columns = readInt16(replication_message, pos, size);

            Int32 data_type_id;
            Int32 type_modifier; /// For example, n in varchar(n)

            bool new_relation_definition = false;
            if (schema_data.find(relation_id) == schema_data.end())
            {
                relation_id_to_name[relation_id] = relation_name;
                schema_data.emplace(relation_id, SchemaData(num_columns));
                new_relation_definition = true;
            }

            auto & current_schema_data = schema_data.find(relation_id)->second;

            if (current_schema_data.number_of_columns != num_columns)
            {
                markTableAsSkipped(relation_id, relation_name);
                return;
            }

            for (uint16_t i = 0; i < num_columns; ++i)
            {
                String column_name;
                readInt8(replication_message, pos, size); /// Marks column as part of replica identity index
                readString(replication_message, pos, size, column_name);

                data_type_id = readInt32(replication_message, pos, size);
                type_modifier = readInt32(replication_message, pos, size);

                if (new_relation_definition)
                {
                    current_schema_data.column_identifiers.emplace_back(std::make_tuple(data_type_id, type_modifier));
                }
                else
                {
                    if (current_schema_data.column_identifiers[i].first != data_type_id
                            || current_schema_data.column_identifiers[i].second != type_modifier)
                    {
                        markTableAsSkipped(relation_id, relation_name);
                        return;
                    }
                }
            }

            tables_to_sync.insert(relation_name);

            break;
        }
        case 'O': // Origin
            break;
        case 'Y': // Type
            break;
        case 'T': // Truncate
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected byte1 value {} while parsing replication message", type);
    }
}


void MaterializedPostgreSQLConsumer::syncTables()
{
    try
    {
        for (const auto & table_name : tables_to_sync)
        {
            auto & buffer = buffers.find(table_name)->second;
            Block result_rows = buffer.description.sample_block.cloneWithColumns(std::move(buffer.columns));

            if (result_rows.rows())
            {
                auto storage = storages[table_name];

                auto insert_context = Context::createCopy(context);
                insert_context->setInternalQuery(true);

                auto insert = std::make_shared<ASTInsertQuery>();
                insert->table_id = storage->getStorageID();
                insert->columns = buffer.columnsAST;

                InterpreterInsertQuery interpreter(insert, insert_context, true);
                auto block_io = interpreter.execute();
                OneBlockInputStream input(result_rows);

                assertBlocksHaveEqualStructure(input.getHeader(), block_io.out->getHeader(), "postgresql replica table sync");
                copyData(input, *block_io.out);

                buffer.columns = buffer.description.sample_block.cloneEmptyColumns();
            }
        }

        LOG_DEBUG(log, "Table sync end for {} tables, last lsn: {} = {}, (attempted lsn {})", tables_to_sync.size(), current_lsn, getLSNValue(current_lsn), getLSNValue(final_lsn));

        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());
        current_lsn = advanceLSN(tx);
        tables_to_sync.clear();
        tx->commit();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


String MaterializedPostgreSQLConsumer::advanceLSN(std::shared_ptr<pqxx::nontransaction> tx)
{
    std::string query_str = fmt::format("SELECT end_lsn FROM pg_replication_slot_advance('{}', '{}')", replication_slot_name, final_lsn);
    pqxx::result result{tx->exec(query_str)};

    final_lsn = result[0][0].as<std::string>();
    LOG_TRACE(log, "Advanced LSN up to: {}", getLSNValue(final_lsn));
    return final_lsn;
}


/// Sync for some table might not be allowed if:
/// 1. Table schema changed and might break synchronization.
/// 2. There is no storage for this table. (As a result of some exception or incorrect pg_publication)
bool MaterializedPostgreSQLConsumer::isSyncAllowed(Int32 relation_id)
{
    auto table_with_lsn = skip_list.find(relation_id);

    /// Table is not present in a skip list - allow synchronization.
    if (table_with_lsn == skip_list.end())
        return true;

    const auto & table_start_lsn = table_with_lsn->second;

    /// Table is in a skip list and has not yet received a valid lsn == it has not been reloaded.
    if (table_start_lsn.empty())
        return false;

    /// Table has received a valid lsn, but it is not yet at a position, from which synchronization is
    /// allowed. It is allowed only after lsn position, returned with snapshot, from which
    /// table was reloaded.
    if (getLSNValue(current_lsn) >= getLSNValue(table_start_lsn))
    {
        LOG_TRACE(log, "Synchronization is resumed for table: {} (start_lsn: {})",
                relation_id_to_name[relation_id], table_start_lsn);

        skip_list.erase(table_with_lsn);

        return true;
    }

    return false;
}


void MaterializedPostgreSQLConsumer::markTableAsSkipped(Int32 relation_id, const String & relation_name)
{
    /// Empty lsn string means - continue waiting for valid lsn.
    skip_list.insert({relation_id, ""});

    if (storages.count(relation_name))
    {
        /// Erase cached schema identifiers. It will be updated again once table is allowed back into replication stream
        /// and it receives first data after update.
        schema_data.erase(relation_id);

        /// Clear table buffer.
        auto & buffer = buffers.find(relation_name)->second;
        buffer.columns = buffer.description.sample_block.cloneEmptyColumns();

        if (allow_automatic_update)
            LOG_TRACE(log, "Table {} (relation_id: {}) is skipped temporarily. It will be reloaded in the background", relation_name, relation_id);
        else
            LOG_WARNING(log, "Table {} (relation_id: {}) is skipped, because table schema has changed", relation_name, relation_id);
    }
}


/// Read binary changes from replication slot via COPY command (starting from current lsn in a slot).
bool MaterializedPostgreSQLConsumer::readFromReplicationSlot()
{
    bool slot_empty = true;

    try
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());

        /// Read up to max_block_size rows changes (upto_n_changes parameter). It might return larger number as the limit
        /// is checked only after each transaction block.
        /// Returns less than max_block_changes, if reached end of wal. Sync to table in this case.

        std::string query_str = fmt::format(
                "select lsn, data FROM pg_logical_slot_peek_binary_changes("
                "'{}', NULL, {}, 'publication_names', '{}', 'proto_version', '1')",
                replication_slot_name, max_block_size, publication_name);

        auto stream{pqxx::stream_from::query(*tx, query_str)};

        while (true)
        {
            const std::vector<pqxx::zview> * row{stream.read_row()};

            if (!row)
            {
                stream.complete();

                if (slot_empty)
                {
                    tx->commit();
                    return false;
                }

                break;
            }

            slot_empty = false;
            current_lsn = (*row)[0];
            lsn_value = getLSNValue(current_lsn);

            processReplicationMessage((*row)[1].c_str(), (*row)[1].size());
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        connection->tryUpdateConnection();
        return false;
    }
    catch (const pqxx::sql_error & e)
    {
        /// For now sql replication interface is used and it has the problem that it registers relcache
        /// callbacks on each pg_logical_slot_get_changes and there is no way to invalidate them:
        /// https://github.com/postgres/postgres/blob/master/src/backend/replication/pgoutput/pgoutput.c#L1128
        /// So at some point will get out of limit and then they will be cleaned.
        std::string error_message = e.what();
        if (error_message.find("out of relcache_callback_list slots") == std::string::npos)
            tryLogCurrentException(__PRETTY_FUNCTION__);

        connection->tryUpdateConnection();
        return false;
    }
    catch (const pqxx::conversion_error & e)
    {
        LOG_ERROR(log, "Conversion error: {}", e.what());
        return false;
    }
    catch (const pqxx::in_doubt_error & e)
    {
        LOG_ERROR(log, "PostgreSQL library has some doubts: {}", e.what());
        return false;
    }
    catch (const pqxx::internal_error & e)
    {
        LOG_ERROR(log, "PostgreSQL library internal error: {}", e.what());
        return false;
    }
    catch (...)
    {
        /// Since reading is done from a background task, it is important to catch any possible error
        /// in order to understand why something does not work.
        try
        {
            std::rethrow_exception(std::current_exception());
        }
        catch (const std::exception& e)
        {
            LOG_ERROR(log, "Unexpected error: {}", e.what());
        }
    }

    if (!tables_to_sync.empty())
        syncTables();

    return true;
}


bool MaterializedPostgreSQLConsumer::consume(std::vector<std::pair<Int32, String>> & skipped_tables)
{
    /// Check if there are tables, which are skipped from being updated by changes from replication stream,
    /// because schema changes were detected. Update them, if it is allowed.
    if (allow_automatic_update && !skip_list.empty())
    {
        for (const auto & [relation_id, lsn] : skip_list)
        {
            /// Non-empty lsn in this place means that table was already updated, but no changes for that table were
            /// received in a previous stream. A table is removed from skip list only when there came
            /// changes for table with lsn higher than lsn of snapshot, from which table was reloaded. Since table
            /// reaload and reading from replication stream are done in the same thread, no lsn will be skipped
            /// between these two events.
            if (lsn.empty())
                skipped_tables.emplace_back(std::make_pair(relation_id, relation_id_to_name[relation_id]));
        }
    }

    /// Read up to max_block_size changed (approximately - in same cases might be more).
    /// false: no data was read, reschedule.
    /// true: some data was read, schedule as soon as possible.
    return readFromReplicationSlot();
}


void MaterializedPostgreSQLConsumer::updateNested(const String & table_name, StoragePtr nested_storage, Int32 table_id, const String & table_start_lsn)
{
    /// Cache new pointer to replacingMergeTree table.
    storages[table_name] = nested_storage;

    /// Create a new empty buffer (with updated metadata), where data is first loaded before syncing into actual table.
    auto & buffer = buffers.find(table_name)->second;
    buffer.createEmptyBuffer(nested_storage);

    /// Set start position to valid lsn. Before it was an empty string. Further read for table allowed, if it has a valid lsn.
    skip_list[table_id] = table_start_lsn;
}

}
