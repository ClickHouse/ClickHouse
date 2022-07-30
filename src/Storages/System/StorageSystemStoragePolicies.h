#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class Context;


/** Implements the system table `storage`, which allows you to get information about all disks.
*/
class StorageSystemStoragePolicies final : public IStorage
{
public:
    explicit StorageSystemStoragePolicies(const StorageID & table_id_);

    std::string getName() const override { return "SystemStoragePolicies"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }
};

}
