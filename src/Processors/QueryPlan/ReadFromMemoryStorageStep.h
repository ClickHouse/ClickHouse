#pragma once

#include <memory>

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

class QueryPipelineBuilder;

class ReadFromMemoryStorageStep final : public SourceStepWithFilter
{
public:
    explicit ReadFromMemoryStorageStep(Pipe pipe_);

    ReadFromMemoryStorageStep() = delete;
    ReadFromMemoryStorageStep(const ReadFromMemoryStorageStep &) = delete;
    ReadFromMemoryStorageStep & operator=(const ReadFromMemoryStorageStep &) = delete;

    ReadFromMemoryStorageStep(ReadFromMemoryStorageStep &&) = default;
    ReadFromMemoryStorageStep & operator=(ReadFromMemoryStorageStep &&) = default;

    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    static Pipe makePipe(const Names & columns_to_read_,
                         const StorageSnapshotPtr & storage_snapshot_,
                         size_t num_streams_,
                         bool delay_read_for_global_sub_queries_);

private:
    static constexpr auto name = "ReadFromMemoryStorage";
    Pipe pipe;
};

}
