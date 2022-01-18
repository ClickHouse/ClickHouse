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

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// Why we may have virtual columns in the storage from a single block?
    /// Because it used as tmp storage for pushing blocks into views, and some
    /// views may contain virtual columns from original storage.
    NamesAndTypesList getVirtuals() const override
    {
        return virtuals;
    }
private:
    Block res_block;
    NamesAndTypesList virtuals;

protected:
    StorageValues(
        const StorageID & table_id_, const ColumnsDescription & columns_, const Block & res_block_, const NamesAndTypesList & virtuals_ = {});
};

}
