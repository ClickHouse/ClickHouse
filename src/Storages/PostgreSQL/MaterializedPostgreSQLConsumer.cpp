#include "MaterializedPostgreSQLConsumer.h"

#include "StorageMaterializedPostgreSQL.h"
#include <Columns/ColumnNullable.h>
#include <base/hex.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Common/SettingsChanges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    using ArrayInfo = std::unordered_map<size_t, PostgreSQLArrayInfo>;

    ArrayInfo createArrayInfos(const NamesAndTypesList & columns, const ExternalResultDescription & columns_description)
    {
        ArrayInfo array_info;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (columns_description.types[i].first == ExternalResultDescription::ValueType::vtArray)
                preparePostgreSQLArrayInfo(array_info, i, columns_description.sample_block.getByPosition(i).type);
        }
        return array_info;
    }
}

MaterializedPostgreSQLConsumer::MaterializedPostgreSQLConsumer(
    ContextPtr context_,
    std::shared_ptr<postgres::Connection> connection_,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const std::string & start_lsn,
    const size_t max_block_size_,
    bool schema_as_a_part_of_table_name_,
    StorageInfos storages_info_,
    const String & name_for_logger)
    : log(getLogger("PostgreSQLReplicaConsumer(" + name_for_logger + ")"))
    , context(context_)
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , connection(connection_)
    , current_lsn(start_lsn)
    , final_lsn(start_lsn)
    , lsn_value(getLSNValue(start_lsn))
    , max_block_size(max_block_size_)
    , schema_as_a_part_of_table_name(schema_as_a_part_of_table_name_)
{
    {
        auto tx = std::make_shared<pqxx::nontransaction>(connection->getRef());
        current_lsn = advanceLSN(tx);
        tx->commit();
    }

    for (const auto & [table_name, storage_info] : storages_info_)
        storages.emplace(table_name, StorageData(storage_info, log));

    LOG_TRACE(log, "Starting replication. LSN: {} (last: {}), storages: {}",
              getLSNValue(current_lsn), getLSNValue(final_lsn), storages.size());
}


MaterializedPostgreSQLConsumer::StorageData::StorageData(const StorageInfo & storage_info, LoggerPtr log_)
    : storage(storage_info.storage)
    , table_description(storage_info.storage->getInMemoryMetadataPtr()->getSampleBlock())
    , columns_attributes(storage_info.attributes)
    , column_names(storage_info.storage->getInMemoryMetadataPtr()->getColumns().getNamesOfPhysical())
    , array_info(createArrayInfos(storage_info.storage->getInMemoryMetadataPtr()->getColumns().getAllPhysical(), table_description))
{
    auto columns_num = table_description.sample_block.columns();
    /// +2 because of _sign and _version columns
    if (columns_attributes.size() + 2 != columns_num)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Columns number mismatch. Attributes: {}, buffer: {}",
                        columns_attributes.size(), columns_num);
    }

    LOG_TRACE(log_, "Adding definition for table {}, structure: {}",
              storage_info.storage->getStorageID().getNameForLogs(),
              table_description.sample_block.dumpStructure());
}

MaterializedPostgreSQLConsumer::StorageData::Buffer::Buffer(
    ColumnsWithTypeAndName && columns_,
    const ExternalResultDescription & table_description_)
{
    if (columns_.end() != std::find_if(
            columns_.begin(), columns_.end(),
            [](const auto & col) { return col.name == "_sign" || col.name == "_version"; }))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                        "PostgreSQL table cannot contain `_sign` or `_version` columns "
                        "as they are reserved for internal usage");
    }

    columns_.push_back(table_description_.sample_block.getByName("_sign"));
    columns_.push_back(table_description_.sample_block.getByName("_version"));

    for (const auto & col : columns_)
    {
        if (!table_description_.sample_block.has(col.name))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Having column {}, but no such column in table ({})",
                            col.name, table_description_.sample_block.dumpStructure());
        }

        const auto & actual_column = table_description_.sample_block.getByName(col.name);
        if (col.type != actual_column.type)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Having column {} of type {}, but expected {}",
                            col.name, col.type->getName(), actual_column.type->getName());
        }
    }

    sample_block = Block(columns_);
    columns = sample_block.cloneEmptyColumns();

    for (const auto & name : sample_block.getNames())
        columns_ast.children.emplace_back(std::make_shared<ASTIdentifier>(name));
}

MaterializedPostgreSQLConsumer::StorageData::Buffer & MaterializedPostgreSQLConsumer::StorageData::getLastBuffer()
{
    if (buffers.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No data buffer for {}",
                        storage->getStorageID().getNameForLogs());
    }

    return *buffers.back();
}

MaterializedPostgreSQLConsumer::StorageData::BufferPtr MaterializedPostgreSQLConsumer::StorageData::popBuffer()
{
    if (buffers.empty())
        return nullptr;

    auto buffer = std::move(buffers.front());
    buffers.pop_front();
    return buffer;
}

void MaterializedPostgreSQLConsumer::StorageData::addBuffer(BufferPtr buffer)
{
    buffers.push_back(std::move(buffer));
}

void MaterializedPostgreSQLConsumer::StorageData::returnBuffer(BufferPtr buffer)
{
    buffers.push_front(std::move(buffer));
}

void MaterializedPostgreSQLConsumer::StorageData::Buffer::assertInsertIsPossible(size_t col_idx) const
{
    if (col_idx >= columns.size())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Attempt to insert into buffer at position: "
                        "{}, but block columns size is {} (full structure: {})",
                        col_idx, columns.size(), sample_block.dumpStructure());
    }
}


void MaterializedPostgreSQLConsumer::insertValue(StorageData & storage_data, const std::string & value, size_t column_idx)
{
    auto & buffer = storage_data.getLastBuffer();
    buffer.assertInsertIsPossible(column_idx);

    const auto & column_type_and_name = buffer.sample_block.getByPosition(column_idx);
    auto & column = buffer.columns[column_idx];

    const size_t column_idx_in_table = storage_data.table_description.sample_block.getPositionByName(column_type_and_name.name);
    const auto & type_description = storage_data.table_description.types[column_idx_in_table];

    try
    {
        if (column_type_and_name.type->isNullable())
        {
            ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*column);
            const auto & data_type = assert_cast<const DataTypeNullable &>(*column_type_and_name.type);

            insertPostgreSQLValue(
                    column_nullable.getNestedColumn(), value, type_description.first,
                    data_type.getNestedType(), storage_data.array_info, column_idx_in_table);

            column_nullable.getNullMapData().emplace_back(0);
        }
        else
        {
            insertPostgreSQLValue(
                *column, value, type_description.first, column_type_and_name.type,
                storage_data.array_info, column_idx_in_table);
        }
    }
    catch (const pqxx::conversion_error & e)
    {
        LOG_ERROR(log, "Conversion failed while inserting PostgreSQL value {}, "
                  "will insert default value. Error: {}", value, e.what());

        insertDefaultPostgreSQLValue(*column, *column_type_and_name.column);
    }
}

void MaterializedPostgreSQLConsumer::insertDefaultValue(StorageData & storage_data, size_t column_idx)
{
    auto & buffer = storage_data.getLastBuffer();
    buffer.assertInsertIsPossible(column_idx);

    const auto & column_type_and_name = buffer.sample_block.getByPosition(column_idx);
    auto & column = buffer.columns[column_idx];

    insertDefaultPostgreSQLValue(*column, *column_type_and_name.column);
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
        result |= static_cast<UInt32>(unhex2(message + pos + 2 * i));
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
    StorageData & storage_data,
    const char * message,
    size_t & pos,
    size_t size,
    PostgreSQLQuery type,
    bool old_value)
{
    Int16 num_columns = readInt16(message, pos, size);

    auto proccess_column_value = [&](Int8 identifier, Int16 column_idx)
    {
        switch (identifier) // NOLINT(bugprone-switch-missing-default-case)
        {
            case 'n': /// NULL
            {
                insertDefaultValue(storage_data, column_idx);
                break;
            }
            case 't': /// Text formatted value
            {
                Int32 col_len = readInt32(message, pos, size);
                String value;
                for (Int32 i = 0; i < col_len; ++i)
                    value += static_cast<char>(readInt8(message, pos, size));

                insertValue(storage_data, value, column_idx);
                break;
            }
            case 'u': /// TOAST value && unchanged at the same time. Actual value is not sent.
            {
                /// TOAST values are not supported. (TOAST values are values that are considered in postgres
                /// to be too large to be stored directly)
                LOG_WARNING(log, "Got TOAST value, which is not supported, default value will be used instead.");
                insertDefaultValue(storage_data, column_idx);
                break;
            }
            case 'b': /// Binary data.
            {
                LOG_WARNING(log, "We do not yet process this format of data, will insert default value");
                insertDefaultValue(storage_data, column_idx);
                break;
            }
            default:
            {
                LOG_WARNING(log, "Unexpected identifier: {}. This is a bug! Please report an issue on github", identifier);
                chassert(false);

                insertDefaultValue(storage_data, column_idx);
                break;
            }
        }
    };

    std::exception_ptr error;
    for (int column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        try
        {
            proccess_column_value(readInt8(message, pos, size), column_idx);
        }
        catch (...)
        {
            LOG_ERROR(log,
                      "Got error while receiving value for column {}, will insert default value. Error: {}",
                      column_idx, getCurrentExceptionMessage(true));

            insertDefaultValue(storage_data, column_idx);
            /// Let's collect only the first exception.
            /// This delaying of error throw is needed because
            /// some errors can be ignored and just logged,
            /// but in this case we need to finish insertion to all columns.
            if (!error)
                error = std::current_exception();
        }
    }

    auto & columns = storage_data.getLastBuffer().columns;
    switch (type)
    {
        case PostgreSQLQuery::INSERT:
        {
            columns[num_columns]->insert(static_cast<Int8>(1));
            columns[num_columns + 1]->insert(lsn_value);

            break;
        }
        case PostgreSQLQuery::DELETE:
        {
            columns[num_columns]->insert(static_cast<Int8>(-1));
            columns[num_columns + 1]->insert(lsn_value);

            break;
        }
        case PostgreSQLQuery::UPDATE:
        {
            /// Process old value in case changed value is a primary key.
            if (old_value)
                columns[num_columns]->insert(static_cast<Int8>(-1));
            else
                columns[num_columns]->insert(static_cast<Int8>(1));

            columns[num_columns + 1]->insert(lsn_value);

            break;
        }
    }

    if (error)
        std::rethrow_exception(error);
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
            const auto & table_name = relation_id_to_name[relation_id];
            if (table_name.empty())
            {
                LOG_ERROR(log, "No table mapping for relation id: {}. It's a bug", relation_id);
                return;
            }

            if (!isSyncAllowed(relation_id, table_name))
                return;

            Int8 new_tuple = readInt8(replication_message, pos, size);
            auto & storage_data = storages.find(table_name)->second;

            if (new_tuple)
                readTupleData(storage_data, replication_message, pos, size, PostgreSQLQuery::INSERT);

            break;
        }
        case 'U': // Update
        {
            Int32 relation_id = readInt32(replication_message, pos, size);
            const auto & table_name = relation_id_to_name[relation_id];
            if (table_name.empty())
            {
                LOG_ERROR(log, "No table mapping for relation id: {}. It's a bug", relation_id);
                return;
            }

            if (!isSyncAllowed(relation_id, table_name))
                return;

            auto & storage_data = storages.find(table_name)->second;

            auto proccess_identifier = [&](Int8 identifier) -> bool
            {
                bool read_next = true;
                switch (identifier) // NOLINT(bugprone-switch-missing-default-case)
                {
                    /// Only if changed column(s) are part of replica identity index (or primary keys if they are used instead).
                    /// In this case, first comes a tuple with old replica identity indexes and all other values will come as
                    /// nulls. Then comes a full new row.
                    case 'K': [[fallthrough]];
                    /// Old row. Only if replica identity is set to full. Does not really make sense to use it as
                    /// it is much more efficient to use replica identity index, but support all possible cases.
                    case 'O':
                    {
                        readTupleData(storage_data, replication_message, pos, size, PostgreSQLQuery::UPDATE, true);
                        break;
                    }
                    case 'N':
                    {
                        /// New row.
                        readTupleData(storage_data, replication_message, pos, size, PostgreSQLQuery::UPDATE);
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
            const auto & table_name = relation_id_to_name[relation_id];
            if (table_name.empty())
            {
                LOG_ERROR(log, "No table mapping for relation id: {}. It's a bug", relation_id);
                return;
            }

            if (!isSyncAllowed(relation_id, table_name))
                return;

             /// 0 or 1 if replica identity is set to full. For now only default replica identity is supported (with primary keys).
            readInt8(replication_message, pos, size);

            auto & storage_data = storages.find(table_name)->second;
            readTupleData(storage_data, replication_message, pos, size, PostgreSQLQuery::DELETE);
            break;
        }
        case 'C': // Commit
        {
            constexpr size_t unused_flags_len = 1;
            constexpr size_t commit_lsn_len = 8;
            constexpr size_t transaction_end_lsn_len = 8;
            constexpr size_t transaction_commit_timestamp_len = 8;
            pos += unused_flags_len + commit_lsn_len + transaction_end_lsn_len + transaction_commit_timestamp_len;

            final_lsn = current_lsn;
            committed = true;
            break;
        }
        case 'R': // Relation
        {
            Int32 relation_id = readInt32(replication_message, pos, size);

            String relation_namespace, relation_name;
            readString(replication_message, pos, size, relation_namespace);
            readString(replication_message, pos, size, relation_name);

            String table_name;
            if (!relation_namespace.empty() && schema_as_a_part_of_table_name)
                table_name = relation_namespace + '.' + relation_name;
            else
                table_name = relation_name;

            if (!relation_id_to_name.contains(relation_id))
                relation_id_to_name[relation_id] = table_name;

            if (!isSyncAllowed(relation_id, relation_name))
                return;

            auto storage_iter = storages.find(table_name);
            if (storage_iter == storages.end())
            {
                /// FIXME: This can happen if we created a publication with this table but then got an exception that this
                /// table has primary key or something else.
                LOG_ERROR(log,
                          "Storage for table {} does not exist, but is included in replication stream. (Storages number: {})"
                          "Please manually remove this table from replication (DETACH TABLE query) to avoid redundant replication",
                          table_name, storages.size());
                markTableAsSkipped(relation_id, table_name);
                return;
            }

            /// 'd' - default (primary key if any)
            /// 'n' - nothing
            /// 'f' - all columns (set replica identity full)
            /// 'i' - user defined index with indisreplident set
            /// Only 'd' and 'i' - are supported.
            char replica_identity = readInt8(replication_message, pos, size);
            if (replica_identity != 'd' && replica_identity != 'i')
            {
                LOG_WARNING(log,
                    "Table has replica identity {} - not supported. A table must have a primary key or a replica identity index",
                    replica_identity);
                markTableAsSkipped(relation_id, table_name);
                return;
            }

            auto log_table_structure_changed = [&](const std::string & reason)
            {
                LOG_INFO(log, "Table structure of the table {} changed ({}), "
                         "will mark it as skipped from replication. "
                         "Please perform manual DETACH and ATTACH of the table to bring it back",
                         table_name, reason);
            };

            Int16 num_columns = readInt16(replication_message, pos, size);

            auto & storage_data = storage_iter->second;
            const auto & description = storage_data.table_description;

            const size_t actual_columns_num = storage_data.getColumnsNum();
            if (size_t(num_columns) > actual_columns_num - 2)
            {
                log_table_structure_changed(fmt::format("received {} columns, expected {}", num_columns, actual_columns_num - 2));
                markTableAsSkipped(relation_id, table_name);
                return;
            }

            Int32 data_type_id;
            Int32 type_modifier; /// For example, n in varchar(n)

            std::set<std::string> all_columns(storage_data.column_names.begin(), storage_data.column_names.end());
            std::set<std::string> received_columns;
            ColumnsWithTypeAndName columns;

            for (uint16_t i = 0; i < num_columns; ++i)
            {
                String column_name;
                readInt8(replication_message, pos, size); /// Marks column as part of replica identity index
                readString(replication_message, pos, size, column_name);

                if (!all_columns.contains(column_name))
                {
                    log_table_structure_changed(fmt::format("column {} is not known", column_name));
                    markTableAsSkipped(relation_id, table_name);
                    return;
                }

                data_type_id = readInt32(replication_message, pos, size);
                type_modifier = readInt32(replication_message, pos, size);

                columns.push_back(description.sample_block.getByName(column_name));
                received_columns.emplace(column_name);

                const auto & attributes_it = storage_data.columns_attributes.find(column_name);
                if (attributes_it == storage_data.columns_attributes.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No column {} in attributes", column_name);

                const auto & attributes = attributes_it->second;
                if (attributes.atttypid != data_type_id || attributes.atttypmod != type_modifier)
                {
                    log_table_structure_changed(fmt::format("column {} has a different type", column_name));
                    markTableAsSkipped(relation_id, table_name);
                    return;
                }
            }


            if (size_t(num_columns) < actual_columns_num)
            {
                std::vector<std::string> absent_columns;
                std::set_difference(
                    all_columns.begin(), all_columns.end(),
                    received_columns.begin(), received_columns.end(), std::back_inserter(absent_columns));

                for (const auto & name : absent_columns)
                {
                    if (name == "_sign" || name == "_version")
                        continue;

                    const auto & attributes_it = storage_data.columns_attributes.find(name);
                    if (attributes_it == storage_data.columns_attributes.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "No column {} in attributes", name);

                    /// Column has a default value or it is a GENERATED columns.
                    if (!attributes_it->second.attr_def.empty())
                        continue;

                    log_table_structure_changed(fmt::format("column {} was not found", name));
                    markTableAsSkipped(relation_id, table_name);
                    return;
                }
            }

            storage_data.addBuffer(std::make_unique<StorageData::Buffer>(std::move(columns), description));
            tables_to_sync.insert(table_name);
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
    size_t synced_tables = 0;
    while (!tables_to_sync.empty())
    {
        auto table_name = *tables_to_sync.begin();
        auto & storage_data = storages.find(table_name)->second;

        while (auto buffer = storage_data.popBuffer())
        {
            Block result_rows = buffer->sample_block.cloneWithColumns(std::move(buffer->columns));
            try
            {
                if (result_rows.rows())
                {
                    auto storage = storage_data.storage;

                    auto insert_context = Context::createCopy(context);
                    insert_context->setInternalQuery(true);

                    auto insert = std::make_shared<ASTInsertQuery>();
                    insert->table_id = storage->getStorageID();
                    insert->columns = std::make_shared<ASTExpressionList>(buffer->columns_ast);

                    InterpreterInsertQuery interpreter(insert, insert_context, true, false, false, false);
                    auto io = interpreter.execute();
                    auto input = std::make_shared<SourceFromSingleChunk>(
                        result_rows.cloneEmpty(), Chunk(result_rows.getColumns(), result_rows.rows()));

                    assertBlocksHaveEqualStructure(input->getPort().getHeader(), io.pipeline.getHeader(), "postgresql replica table sync");
                    io.pipeline.complete(Pipe(std::move(input)));

                    CompletedPipelineExecutor executor(io.pipeline);
                    executor.execute();
                    ++synced_tables;
                }
            }
            catch (...)
            {
                /// Retry this buffer later.
                buffer->columns = result_rows.mutateColumns();
                storage_data.returnBuffer(std::move(buffer));
                throw;
            }
        }

        tables_to_sync.erase(tables_to_sync.begin());
    }

    LOG_DEBUG(log, "Table sync end for {} tables, last lsn: {} = {}, (attempted lsn {})",
              synced_tables, current_lsn, getLSNValue(current_lsn), getLSNValue(final_lsn));

    updateLsn();
}

void MaterializedPostgreSQLConsumer::updateLsn()
{
    try
    {
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
    committed = false;
    return final_lsn;
}

/// Sync for some table might not be allowed if:
/// 1. Table schema changed and might break synchronization.
/// 2. There is no storage for this table. (As a result of some exception or incorrect pg_publication)
/// 3. A new table was added to replication, it was loaded via snapshot, but consumer has not yet
///    read wal up to the lsn position of snapshot, from which table was loaded.
bool MaterializedPostgreSQLConsumer::isSyncAllowed(Int32 relation_id, const String & relation_name)
{
    if (deleted_tables.contains(relation_name))
        return false;

    auto new_table_with_lsn = waiting_list.find(relation_name);

    if (new_table_with_lsn != waiting_list.end())
    {
        auto table_start_lsn = new_table_with_lsn->second;
        assert(!table_start_lsn.empty());

        if (getLSNValue(current_lsn) >= getLSNValue(table_start_lsn))
        {
            LOG_TRACE(log, "Synchronization is started for table: {} (start_lsn: {})", relation_name, table_start_lsn);
            waiting_list.erase(new_table_with_lsn);
            return true;
        }
    }

    auto skipped_table_with_lsn = skip_list.find(relation_id);

    /// Table is not present in a skip list - allow synchronization.
    if (skipped_table_with_lsn == skip_list.end())
    {
        return true;
    }

    const auto & table_start_lsn = skipped_table_with_lsn->second;

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

        skip_list.erase(skipped_table_with_lsn);

        return true;
    }

    return false;
}

void MaterializedPostgreSQLConsumer::markTableAsSkipped(Int32 relation_id, const String & relation_name)
{
    skip_list.insert({relation_id, ""}); /// Empty lsn string means - continue waiting for valid lsn.
    storages.erase(relation_name);
    LOG_WARNING(
        log,
        "Table {} is skipped from replication stream because its structure has changes. "
        "Please detach this table and reattach to resume the replication (relation id: {})",
        relation_name, relation_id);
}

void MaterializedPostgreSQLConsumer::addNested(
    const String & postgres_table_name, StorageInfo nested_storage_info, const String & table_start_lsn)
{
    assert(!storages.contains(postgres_table_name));
    storages.emplace(postgres_table_name, StorageData(nested_storage_info, log));

    auto it = deleted_tables.find(postgres_table_name);
    if (it != deleted_tables.end())
        deleted_tables.erase(it);

    /// Replication consumer will read wall and check for currently processed table whether it is allowed to start applying
    /// changes to this table.
    waiting_list[postgres_table_name] = table_start_lsn;
}

void MaterializedPostgreSQLConsumer::updateNested(const String & table_name, StorageInfo nested_storage_info, Int32 table_id, const String & table_start_lsn)
{
    assert(!storages.contains(table_name));
    storages.emplace(table_name, StorageData(nested_storage_info, log));

    /// Set start position to valid lsn. Before it was an empty string. Further read for table allowed, if it has a valid lsn.
    skip_list[table_id] = table_start_lsn;
}

void MaterializedPostgreSQLConsumer::removeNested(const String & postgres_table_name)
{
    auto it = storages.find(postgres_table_name);
    if (it != storages.end())
        storages.erase(it);
    deleted_tables.insert(postgres_table_name);
}

void MaterializedPostgreSQLConsumer::setSetting(const SettingChange & setting)
{
    if (setting.name == "materialized_postgresql_max_block_size")
        max_block_size = setting.value.safeGet<UInt64>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported setting: {}", setting.name);
}

/// Read binary changes from replication slot via COPY command (starting from current lsn in a slot).
bool MaterializedPostgreSQLConsumer::consume()
{
    if (!tables_to_sync.empty())
    {
        syncTables();
    }

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

            try
            {
                /// LOG_DEBUG(log, "Current message: {}", (*row)[1]);
                processReplicationMessage((*row)[1].c_str(), (*row)[1].size());
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::POSTGRESQL_REPLICATION_INTERNAL_ERROR)
                    continue;

                throw;
            }
        }
    }
    catch (const Exception &)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
    catch (const pqxx::broken_connection &)
    {
        LOG_DEBUG(log, "Connection was broken");
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
    catch (const pqxx::internal_error & e)
    {
        LOG_ERROR(log, "PostgreSQL library internal error: {}", e.what());
        return false;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    if (!tables_to_sync.empty())
    {
        syncTables();
    }
    else if (committed)
    {
        updateLsn();
    }

    return true;
}

}
