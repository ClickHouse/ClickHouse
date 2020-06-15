#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns final : public ext::shared_ptr_helper<StorageSystemColumns>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemColumns>;
public:
    std::string getName() const override { return "SystemColumns"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemColumns(const std::string & name_);
};

}
