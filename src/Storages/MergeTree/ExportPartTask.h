#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreePartExportManifest.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class ExportPartTask : public IExecutableTask
{
public:
    explicit ExportPartTask(
        MergeTreeData & storage_,
        const MergeTreePartExportManifest & manifest_,
        ContextPtr context_);
    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override;
    String getQueryId() const override;

    void cancel() noexcept override;

private:
    MergeTreeData & storage;
    MergeTreePartExportManifest manifest;
    ContextPtr local_context;
    QueryPipeline pipeline;
    std::atomic<bool> cancel_requested = false;

    bool isCancelled() const;
};

}
