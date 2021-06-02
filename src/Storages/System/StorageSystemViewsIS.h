#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

// Implements information_schema table 'views'

class StorageSystemViewsIS final : public ext::shared_ptr_helper<StorageSystemViewsIS>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemViewsIS>;
public:
    std::string getName() const override { return "ViewsIS"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemViewsIS(const StorageID & tables_id_);
};

}
