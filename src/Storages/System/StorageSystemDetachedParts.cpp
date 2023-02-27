#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

StorageSystemDetachedParts::StorageSystemDetachedParts(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{{
        {"database",         std::make_shared<DataTypeString>()},
        {"table",            std::make_shared<DataTypeString>()},
        {"partition_id",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name",             std::make_shared<DataTypeString>()},
        {"bytes_on_disk",    std::make_shared<DataTypeUInt64>()},
        {"disk",             std::make_shared<DataTypeString>()},
        {"path",             std::make_shared<DataTypeString>()},
        {"reason",           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"level",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
    }});
    setInMemoryMetadata(storage_metadata);
}
static void calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from, UInt64 & total_size)
{
    /// Files or directories of detached part may not exist. Only count the size of existing files.
    if (disk->isFile(from))
    {
        total_size += disk->getFileSize(from);
    }
    else
    {
        for (auto it = disk->iterateDirectory(from); it->isValid(); it->next())
            calculateTotalSizeOnDiskImpl(disk, fs::path(from) / it->name(), total_size);
    }
}

static UInt64 calculateTotalSizeOnDisk(const DiskPtr & disk, const String & from)
{
    UInt64 total_size = 0;
    try
    {
        calculateTotalSizeOnDiskImpl(disk, from, total_size);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return total_size;
}

Pipe StorageSystemDetachedParts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    StoragesInfoStream stream(query_info, context);

    /// Create the result.
    Block block = storage_snapshot->metadata->getSampleBlock();

    NameSet names_set(column_names.begin(), column_names.end());
    std::vector<UInt8> columns_mask(block.columns());
    Block header;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (names_set.contains(block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(block.getByPosition(i));
        }
    }

    MutableColumns new_columns = header.cloneEmptyColumns();
    while (StoragesInfo info = stream.next())
    {
        const auto parts = info.data->getDetachedParts();
        for (const auto & p : parts)
        {
            size_t src_index = 0, res_index = 0;
            String detached_part_path = fs::path(MergeTreeData::DETACHED_DIR_NAME) / p.dir_name;
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(info.database);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(info.table);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.partition_id : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.dir_name);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(calculateTotalSizeOnDisk(p.disk, fs::path(info.data->getRelativeDataPath()) / detached_part_path));
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.disk->getName());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert((fs::path(info.data->getFullPathOnDisk(p.disk)) / detached_part_path).string());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.prefix : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.min_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.max_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.level : Field());
        }
    }

    UInt64 num_rows = new_columns.at(0)->size();
    Chunk chunk(std::move(new_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
