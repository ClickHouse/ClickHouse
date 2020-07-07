#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/OneBlockInputStream.h>
#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{

/**
  * Implements system table 'detached_parts' which allows to get information
  * about detached data parts for tables of MergeTree family.
  * We don't use StorageSystemPartsBase, because it introduces virtual _state
  * column and column aliases which we don't need.
  */
class StorageSystemDetachedParts final :
    public ext::shared_ptr_helper<StorageSystemDetachedParts>,
    public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemDetachedParts>;
public:
    std::string getName() const override { return "SystemDetachedParts"; }

protected:
    explicit StorageSystemDetachedParts()
        : IStorage({"system", "detached_parts"})
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

    Pipes read(
        const Names & /* column_names */,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/) override
    {
        StoragesInfoStream stream(query_info, context);

        /// Create the result.
        Block block = metadata_snapshot->getSampleBlock();
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
                new_columns[i++]->insert(p.disk);
                new_columns[i++]->insert(p.valid_name ? p.prefix : Field());
                new_columns[i++]->insert(p.valid_name ? p.min_block : Field());
                new_columns[i++]->insert(p.valid_name ? p.max_block : Field());
                new_columns[i++]->insert(p.valid_name ? p.level : Field());
            }
        }

        UInt64 num_rows = new_columns.at(0)->size();
        Chunk chunk(std::move(new_columns), num_rows);

        Pipes pipes;
        pipes.emplace_back(std::make_shared<SourceFromSingleChunk>(std::move(block), std::move(chunk)));
        return pipes;
    }
};

StoragePtr
createDetachedPartsTable()
{
    return StorageSystemDetachedParts::create();
}

}
