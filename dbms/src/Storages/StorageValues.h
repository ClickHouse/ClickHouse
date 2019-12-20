#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{
/* One block storage used for values table function
 * It's structure is similar to IStorageSystemOneBlock
 */
class StorageValues : public ext::shared_ptr_helper<StorageValues>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageValues>;
public:
    std::string getName() const override { return "Values"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::string database_name;
    std::string table_name;
    Block res_block;

protected:
    StorageValues(const std::string & database_name_, const std::string & table_name_, const ColumnsDescription & columns_, const Block & res_block_);
};

}
