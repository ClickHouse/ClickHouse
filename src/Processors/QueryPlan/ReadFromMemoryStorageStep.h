#pragma once

#include <memory>

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class QueryPipelineBuilder;

struct MemorySourceFilter;
using MemorySourceFilterPtr = std::shared_ptr<const MemorySourceFilter>;

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
    ReadFromMemoryStorageStep(const ReadFromMemoryStorageStep &) = default;
    ReadFromMemoryStorageStep & operator=(const ReadFromMemoryStorageStep &) = delete;

    ReadFromMemoryStorageStep(ReadFromMemoryStorageStep &&) = default;
    ReadFromMemoryStorageStep & operator=(ReadFromMemoryStorageStep &&) = delete;

    String getName() const override { return name; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value) override;

    bool supportsTopKDynamicFilter(const ColumnWithTypeAndName & sort_column) const override;
    void setTopKFilter(FormatTopKFilterInfoPtr info) override;

    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

    QueryPlanStepPtr clone() const override;

    const StoragePtr & getStorage() const { return storage; }

private:
    static constexpr auto name = "ReadFromMemoryStorage";

    Names columns_to_read;
    StoragePtr storage;
    size_t num_streams;
    bool delay_read_for_global_sub_queries;

    /// Whether `applyFilters` has run for this step, i.e. whether the sets of the in-source filters
    /// were built in place; see `updatePrewhereInfo`.
    bool filters_applied = false;

    /// TopN dynamic filtering (see `tryOptimizeTopK`): the rows that cannot enter the top-K heap
    /// of the query are dropped inside the source, before the other columns are read.
    FormatTopKFilterInfoPtr top_k_filter;

    /// In-source filtering (TopN threshold, row-level security filter, PREWHERE),
    /// or nullptr when there is nothing to apply.
    MemorySourceFilterPtr makeSourceFilter(const NamesAndTypesList & physical_columns) const;

    Pipe makePipe();
};

}
