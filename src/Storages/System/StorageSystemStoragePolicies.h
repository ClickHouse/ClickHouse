#pragma once

#include <ext/shared_ptr_helper.h>
#include <Formats/FormatSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class Context;


/** Implements the system table `storage`, which allows you to get information about all disks.
*/
class StorageSystemStoragePolicies final : public ext::shared_ptr_helper<StorageSystemStoragePolicies>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageSystemStoragePolicies>;
public:
    std::string getName() const override { return "SystemStoragePolicies"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemStoragePolicies(const std::string & name_);
};

}
