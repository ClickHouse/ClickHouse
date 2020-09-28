#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MySQLBuffer.h>

#include <Core/Names.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <Databases/MySQL/MySQLBuffer.h>
#include <Databases/MySQL/MySQLUtils.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

void MySQLBuffer::add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes)
{
    total_blocks_rows += written_rows;
    total_blocks_bytes += written_bytes;
    max_block_rows = std::max(block_rows, max_block_rows);
    max_block_bytes = std::max(block_bytes, max_block_bytes);
}

bool MySQLBuffer::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const
{
    return max_block_rows >= check_block_rows || max_block_bytes >= check_block_bytes || total_blocks_rows >= check_total_rows
        || total_blocks_bytes >= check_total_bytes;
}

void MySQLBuffer::commit(const Context & context)
{
    try
    {
        for (auto & table_name_and_buffer : data)
        {
            Context query_context = createQueryContext(context);
            OneBlockInputStream input(table_name_and_buffer.second->first);
            BlockOutputStreamPtr out = getTableOutput(database, table_name_and_buffer.first, query_context, true);
            copyData(input, *out);
        }

        data.clear();
        max_block_rows = 0;
        max_block_bytes = 0;
        total_blocks_rows = 0;
        total_blocks_bytes = 0;
    }
    catch (...)
    {
        data.clear();
        throw;
    }
}

MySQLBuffer::BufferAndSortingColumnsPtr MySQLBuffer::getTableDataBuffer(
    const String & table_name, const Context & context)
{
    const auto & iterator = data.find(table_name);
    if (iterator == data.end())
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(database, table_name), context);

        const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
        BufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
            table_name, std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

        Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

        for (const auto & required_name_for_sorting_key : required_for_sorting_key)
            buffer_and_soring_columns->second.emplace_back(
                buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));

        return buffer_and_soring_columns;
    }

    return iterator->second;
}

}

#endif
