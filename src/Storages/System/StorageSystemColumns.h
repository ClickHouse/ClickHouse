#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns final : public shared_ptr_helper<StorageSystemColumns>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemColumns>;
public:
    std::string getName() const override { return "SystemColumns"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemColumns(const StorageID & table_id_);
};

}
