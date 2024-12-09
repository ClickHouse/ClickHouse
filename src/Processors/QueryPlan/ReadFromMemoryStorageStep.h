#pragma once

#include <memory>

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class QueryPipelineBuilder;

class ReadFromMemoryStorageStep final : public SourceStepWithFilter
{
public:
    ReadFromMemoryStorageStep(
        const Names & columns_to_read_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        StoragePtr storage_,
        size_t num_streams_,
        bool delay_read_for_global_sub_queries_);

    ReadFromMemoryStorageStep() = delete;
    ReadFromMemoryStorageStep(const ReadFromMemoryStorageStep &) = delete;
    ReadFromMemoryStorageStep & operator=(const ReadFromMemoryStorageStep &) = delete;

    ReadFromMemoryStorageStep(ReadFromMemoryStorageStep &&) = default;
    ReadFromMemoryStorageStep & operator=(ReadFromMemoryStorageStep &&) = default;

    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const StoragePtr & getStorage() const { return storage; }

private:
    static constexpr auto name = "ReadFromMemoryStorage";

    Names columns_to_read;
    StoragePtr storage;
    size_t num_streams;
    bool delay_read_for_global_sub_queries;

    Pipe makePipe();
};

}
