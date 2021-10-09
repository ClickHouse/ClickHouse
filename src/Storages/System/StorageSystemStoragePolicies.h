#pragma once

#include <base/shared_ptr_helper.h>
#include <Formats/FormatSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class Context;


/** Implements the system table `storage`, which allows you to get information about all disks.
*/
class StorageSystemStoragePolicies final : public shared_ptr_helper<StorageSystemStoragePolicies>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemStoragePolicies>;
public:
    std::string getName() const override { return "SystemStoragePolicies"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemStoragePolicies(const StorageID & table_id_);
};

}
