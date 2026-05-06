#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreePartExportManifest.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ExportPartTask.h>

namespace DB
{

/*
    Decorator around the ExportPartTask to lock the part inside the task
*/
class ExportPartFromPartitionExportTask : public IExecutableTask
{
public:
    explicit ExportPartFromPartitionExportTask(
        StorageReplicatedMergeTree & storage_,
        const std::string & key_,
        const MergeTreePartExportManifest & manifest_);
    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override;
    String getQueryId() const override;

    void cancel() noexcept override;

private:
    StorageReplicatedMergeTree & storage;
    std::string key;
    MergeTreePartExportManifest manifest;
    std::shared_ptr<ExportPartTask> export_part_task;
};

}
