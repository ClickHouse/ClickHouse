#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
class ReadFromStreamLikeEngine : public ISourceStep, protected WithContext
{
public:
    ReadFromStreamLikeEngine(
        const Names & column_names_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::shared_ptr<const StorageLimitsList> storage_limits_,
        ContextPtr context_);

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/) final;

protected:
    virtual Pipe makePipe() = 0;

    std::shared_ptr<const StorageLimitsList> storage_limits;
};
}
