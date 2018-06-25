#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/// Implements the `mutations` system table, which provides information about the status of mutations
/// in the MergeTree tables.
class StorageSystemMutations : public ext::shared_ptr_helper<StorageSystemMutations>, public IStorage
{
public:
    String getName() const override { return "SystemMutations"; }
    String getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const String name;

protected:
    StorageSystemMutations(const String & name_);
};

}
