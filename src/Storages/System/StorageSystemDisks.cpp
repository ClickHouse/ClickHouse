#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemDisks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
}


StorageSystemDisks::StorageSystemDisks(const std::string & name_)
    : IStorage({"system", name_})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"free_space", std::make_shared<DataTypeUInt64>()},
        {"total_space", std::make_shared<DataTypeUInt64>()},
        {"keep_free_space", std::make_shared<DataTypeUInt64>()},
        {"type", std::make_shared<DataTypeString>()},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipes StorageSystemDisks::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_path = ColumnString::create();
    MutableColumnPtr col_free = ColumnUInt64::create();
    MutableColumnPtr col_total = ColumnUInt64::create();
    MutableColumnPtr col_keep = ColumnUInt64::create();
    MutableColumnPtr col_type = ColumnString::create();

    for (const auto & [disk_name, disk_ptr] : context.getDisksMap())
    {
        col_name->insert(disk_name);
        col_path->insert(disk_ptr->getPath());
        col_free->insert(disk_ptr->getAvailableSpace());
        col_total->insert(disk_ptr->getTotalSpace());
        col_keep->insert(disk_ptr->getKeepingFreeSpace());
        col_type->insert(disk_ptr->getType());
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_name));
    res_columns.emplace_back(std::move(col_path));
    res_columns.emplace_back(std::move(col_free));
    res_columns.emplace_back(std::move(col_total));
    res_columns.emplace_back(std::move(col_keep));
    res_columns.emplace_back(std::move(col_type));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    Pipes pipes;
    pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), std::move(chunk)));

    return pipes;
}

}
