#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{
/* One block storage used for values table function
 * It's structure is similar to IStorageSystemOneBlock
 */
class StorageValues final : public ext::shared_ptr_helper<StorageValues>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageValues>;
public:
    std::string getName() const override { return "Values"; }

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    Block res_block;

protected:
    StorageValues(StorageID table_id_, ColumnsDescription columns_, Block res_block_);
};

}
