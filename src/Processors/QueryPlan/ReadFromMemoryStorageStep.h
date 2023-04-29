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
    ReadFromMemoryStorageStep(QueryPlan & query_plan,
                              const SelectQueryInfo & query_info,
                              ContextPtr context,
                              const Names & column_names,
                              const StorageSnapshotPtr & storage_snapshot,
                              size_t num_streams,
                              bool delay_read_for_global_sub_queries);

    ReadFromMemoryStorageStep() = delete;
    ReadFromMemoryStorageStep(const ReadFromMemoryStorageStep &) = delete;
    ReadFromMemoryStorageStep & operator=(const ReadFromMemoryStorageStep &) = delete;

    ReadFromMemoryStorageStep(ReadFromMemoryStorageStep &&) = delete;
    ReadFromMemoryStorageStep & operator=(ReadFromMemoryStorageStep &&) = delete;

    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    static constexpr auto name = "ReadFromMemoryStorage";

    QueryPlan & query_plan;
    const SelectQueryInfo & query_info;
    ContextPtr context;

    Names columns_to_read;
    StorageSnapshotPtr storage_snapshot;
    size_t num_streams;
    bool delay_read_for_global_sub_queries;

    Pipe makePipe();
};

}
