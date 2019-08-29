#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/OneBlockInputStream.h>
#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Storages/System/StorageSystemPartsBase.h>

namespace DB
{

/**
  * Implements system table 'detached_parts' which allows to get information
  * about detached data parts for tables of MergeTree family.
  * We don't use StorageSystemPartsBase, because it introduces virtual _state
  * column and column aliases which we don't need.
  */
class StorageSystemDetachedParts :
    public ext::shared_ptr_helper<StorageSystemDetachedParts>,
    public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemDetachedParts>;
public:
    std::string getName() const override { return "SystemDetachedParts"; }
    std::string getTableName() const override { return "detached_parts"; }
    std::string getDatabaseName() const override { return "system"; }

protected:
    explicit StorageSystemDetachedParts()
    {
        setColumns(ColumnsDescription{{
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"partition_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"name", std::make_shared<DataTypeString>()},
            {"reason", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
            {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
            {"level", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
        }});
    }

    BlockInputStreams read(
            const Names & /* column_names */,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum /*processed_stage*/,
            const size_t /*max_block_size*/,
            const unsigned /*num_streams*/) override
    {
        StoragesInfoStream stream(query_info, context);

        /// Create the result.
        Block block = getSampleBlock();
        MutableColumns new_columns = block.cloneEmptyColumns();

        while (StoragesInfo info = stream.next())
        {
            const auto parts = info.data->getDetachedParts();
            for (auto & p : parts)
            {
                size_t i = 0;
                new_columns[i++]->insert(info.database);
                new_columns[i++]->insert(info.table);
                new_columns[i++]->insert(p.valid_name ? p.partition_id : Field());
                new_columns[i++]->insert(p.dir_name);
                new_columns[i++]->insert(p.valid_name ? p.prefix : Field());
                new_columns[i++]->insert(p.valid_name ? p.min_block : Field());
                new_columns[i++]->insert(p.valid_name ? p.max_block : Field());
                new_columns[i++]->insert(p.valid_name ? p.level : Field());
            }
        }

        return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(
             block.cloneWithColumns(std::move(new_columns))));
    }
};

StoragePtr
createDetachedPartsTable()
{
    return StorageSystemDetachedParts::create();
}

}
