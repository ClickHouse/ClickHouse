#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements infromation_table table 'columns'
  */
class StorageSystemColumnsIS final : public shared_ptr_helper<StorageSystemColumnsIS>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemColumnsIS>;
public:
    std::string getName() const override { return "SystemColumnsIS"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemColumnsIS(const StorageID & table_id_);
};

}
