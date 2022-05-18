#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/IStorage.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{

StorageSystemDetachedParts::StorageSystemDetachedParts(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{{
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name", std::make_shared<DataTypeString>()},
        {"disk", std::make_shared<DataTypeString>()},
        {"reason", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"level", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
    }});
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDetachedParts::read(
    const Names & /* column_names */,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    StoragesInfoStream stream(query_info, context);

    /// Create the result.
    Block block = storage_snapshot->metadata->getSampleBlock();
    MutableColumns new_columns = block.cloneEmptyColumns();

    while (StoragesInfo info = stream.next())
    {
        const auto parts = info.data->getDetachedParts();
        for (const auto & p : parts)
        {
            size_t i = 0;
            new_columns[i++]->insert(info.database);
            new_columns[i++]->insert(info.table);
            new_columns[i++]->insert(p.valid_name ? p.partition_id : Field());
            new_columns[i++]->insert(p.dir_name);
            new_columns[i++]->insert(p.disk->getName());
            new_columns[i++]->insert(p.valid_name ? p.prefix : Field());
            new_columns[i++]->insert(p.valid_name ? p.min_block : Field());
            new_columns[i++]->insert(p.valid_name ? p.max_block : Field());
            new_columns[i++]->insert(p.valid_name ? p.level : Field());
        }
    }

    UInt64 num_rows = new_columns.at(0)->size();
    Chunk chunk(std::move(new_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(block), std::move(chunk)));
}

}
