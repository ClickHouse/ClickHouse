#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MySQLBuffer.h>

#include <memory>

#include <Core/Names.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Databases/MySQL/MySQLUtils.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMySQLReplica.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Parsers/ASTInsertQuery.h>

namespace DB
{

void IMySQLBuffer::add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes)
{
    total_blocks_rows += written_rows;
    total_blocks_bytes += written_bytes;
    max_block_rows = std::max(block_rows, max_block_rows);
    max_block_bytes = std::max(block_bytes, max_block_bytes);
}

void IMySQLBuffer::flushCounters()
{
    max_block_rows = 0;
    max_block_bytes = 0;
    total_blocks_rows = 0;
    total_blocks_bytes = 0;
}

bool IMySQLBuffer::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const
{
    return max_block_rows >= check_block_rows || max_block_bytes >= check_block_bytes || total_blocks_rows >= check_total_rows
        || total_blocks_bytes >= check_total_bytes;
}

void MySQLDatabaseBuffer::commit(const Context & context)
{
    try
    {
        for (auto & table_name_and_buffer : data)
        {
            Context query_context = createQueryContext(context);
            OneBlockInputStream input(table_name_and_buffer.second->first);
            BlockOutputStreamPtr out = getTableOutput(database_name, table_name_and_buffer.first, query_context, true);
            copyData(input, *out);
        }

        data.clear();
        flushCounters();
    }
    catch (...)
    {
        data.clear();
        throw;
    }
}

MySQLBufferAndSortingColumnsPtr MySQLDatabaseBuffer::getTableDataBuffer(
    const String & table_name,
    const Context & context)
{
    const auto & iterator = data.find(table_name);
    if (iterator == data.end())
    {
        StoragePtr storage = DatabaseCatalog::instance()
            .getTable(StorageID(database_name, table_name), context);

        const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
        MySQLBufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
            table_name,
            std::make_shared<MySQLBufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

        Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

        for (const auto & required_name_for_sorting_key : required_for_sorting_key)
            buffer_and_soring_columns->second.emplace_back(
                buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));

        return buffer_and_soring_columns;
    }

    return iterator->second;
}

void MySQLStorageBuffer::commit(const Context & context)
{
    Context query_context = createQueryContext(context);

    if (table_id.database_name.empty())
    {
        throw Exception(
            "Empty database_name in table_id.",
            ErrorCodes::LOGICAL_ERROR);
    }

    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, query_context);
    if (!storage)
    {
        throw Exception(
            "Engine table " + table_id.getNameForLogs() + " doesn't exist.",
            ErrorCodes::LOGICAL_ERROR);
    }

    auto mysql_replica_storage = std::dynamic_pointer_cast<StorageMySQLReplica>(storage);
    if (!mysql_replica_storage)
    {
        throw Exception(
            "Tablewise replication is supported only for StorageMySQLReplica.",
            ErrorCodes::LOGICAL_ERROR);
    }

    if (!data)
    {
        return;
    }

    auto insert = std::make_shared<ASTInsertQuery>();
    auto out_stream = mysql_replica_storage->storage_internal->write(
        insert,
        mysql_replica_storage->getInMemoryMetadataPtr(),
        context);
    out_stream->write(data->first);

    data.reset();
    flushCounters();
}

MySQLBufferAndSortingColumnsPtr MySQLStorageBuffer::getTableDataBuffer(
    const String & table_name,
    const Context & context)
{
    if (table_id.database_name.empty())
    {
        throw Exception(
            "Empty database_name in table_id.",
            ErrorCodes::LOGICAL_ERROR);
    }

    StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, context);
    if (!storage)
    {
        throw Exception(
            "Engine table " + table_id.getNameForLogs() + " doesn't exist.",
            ErrorCodes::LOGICAL_ERROR);
    }

    if (table_name != mysql_table_name)
    {
        return std::shared_ptr<MySQLBufferAndSortingColumns>();
    }

    const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
    data = std::make_shared<MySQLBufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{});

    Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

    for (const auto & required_name_for_sorting_key : required_for_sorting_key)
    {
        data->second.emplace_back(
            data->first.getPositionByName(required_name_for_sorting_key));
    }

    return data;
}

}

#endif
