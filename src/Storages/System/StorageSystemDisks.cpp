#include <Storages/System/StorageSystemDisks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
}


StorageSystemDisks::StorageSystemDisks(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"free_space", std::make_shared<DataTypeUInt64>()},
        {"total_space", std::make_shared<DataTypeUInt64>()},
        {"unreserved_space", std::make_shared<DataTypeUInt64>()},
        {"keep_free_space", std::make_shared<DataTypeUInt64>()},
        {"type", std::make_shared<DataTypeString>()},
        {"is_encrypted", std::make_shared<DataTypeUInt8>()},
        {"cache_path", std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDisks::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_path = ColumnString::create();
    MutableColumnPtr col_free = ColumnUInt64::create();
    MutableColumnPtr col_total = ColumnUInt64::create();
    MutableColumnPtr col_unreserved = ColumnUInt64::create();
    MutableColumnPtr col_keep = ColumnUInt64::create();
    MutableColumnPtr col_type = ColumnString::create();
    MutableColumnPtr col_is_encrypted = ColumnUInt8::create();
    MutableColumnPtr col_cache_path = ColumnString::create();

    for (const auto & [disk_name, disk_ptr] : context->getDisksMap())
    {
        col_name->insert(disk_name);
        col_path->insert(disk_ptr->getPath());
        col_free->insert(disk_ptr->getAvailableSpace());
        col_total->insert(disk_ptr->getTotalSpace());
        col_unreserved->insert(disk_ptr->getUnreservedSpace());
        col_keep->insert(disk_ptr->getKeepingFreeSpace());
        auto data_source_description = disk_ptr->getDataSourceDescription();
        col_type->insert(toString(data_source_description.type));
        col_is_encrypted->insert(data_source_description.is_encrypted);

        String cache_path;
        if (disk_ptr->supportsCache())
            cache_path = disk_ptr->getCacheBasePath();

        col_cache_path->insert(cache_path);
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_name));
    res_columns.emplace_back(std::move(col_path));
    res_columns.emplace_back(std::move(col_free));
    res_columns.emplace_back(std::move(col_total));
    res_columns.emplace_back(std::move(col_unreserved));
    res_columns.emplace_back(std::move(col_keep));
    res_columns.emplace_back(std::move(col_type));
    res_columns.emplace_back(std::move(col_is_encrypted));
    res_columns.emplace_back(std::move(col_cache_path));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
