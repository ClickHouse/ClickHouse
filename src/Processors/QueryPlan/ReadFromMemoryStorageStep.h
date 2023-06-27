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
    using FixedColumns = std::unordered_set<const ActionsDAG::Node *>;

    ReadFromMemoryStorageStep(const Names & columns_to_read_,
                              const StorageSnapshotPtr & storage_snapshot_,
                              size_t num_streams_,
                              bool delay_read_for_global_sub_queries_);

    ReadFromMemoryStorageStep() = delete;
    ReadFromMemoryStorageStep(const ReadFromMemoryStorageStep &) = delete;
    ReadFromMemoryStorageStep & operator=(const ReadFromMemoryStorageStep &) = delete;

    ReadFromMemoryStorageStep(ReadFromMemoryStorageStep &&) = default;
    ReadFromMemoryStorageStep & operator=(ReadFromMemoryStorageStep &&) = default;

    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    static constexpr auto name = "ReadFromMemoryStorage";

    Names columns_to_read;
    StorageSnapshotPtr storage_snapshot;
    size_t num_streams;
    bool delay_read_for_global_sub_queries;

    [[maybe_unused]]
    FixedColumns makeFixedColumns();

    Pipe makePipe();

    std::shared_ptr<const Blocks> filteredByFixedColumns(std::shared_ptr<const Blocks> /*blocks_ptr*/);
};

}
