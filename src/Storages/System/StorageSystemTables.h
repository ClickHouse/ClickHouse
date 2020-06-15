#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables final : public ext::shared_ptr_helper<StorageSystemTables>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemTables>;
public:
    std::string getName() const override { return "SystemTables"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemTables(const std::string & name_);
};

}
