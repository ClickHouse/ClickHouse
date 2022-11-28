#pragma once

#include <base/shared_ptr_helper.h>
#include <Formats/FormatSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class Context;


/** Implements the system table `disks`, which allows you to get information about all disks.
*/
class StorageSystemDisks final : public shared_ptr_helper<StorageSystemDisks>, public IStorage
{
    friend struct shared_ptr_helper<StorageSystemDisks>;
public:
    std::string getName() const override { return "SystemDisks"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemDisks(const StorageID & table_id_);
};

}
