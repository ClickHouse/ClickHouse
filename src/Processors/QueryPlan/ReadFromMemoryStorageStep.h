#pragma once

#include <memory>

#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class QueryPipelineBuilder;

struct MemorySourceFilter;
using MemorySourceFilterPtr = std::shared_ptr<const MemorySourceFilter>;

class LazilyReadFromMemoryStorage;

/// Shared state between the two branches of lazy materialization for the `Memory` storage.
/// Both branches read the blocks of the same storage snapshot, so the global row index of a row
/// is simply its number in the snapshot: the rows of the preceding blocks plus the row in its block.
struct MemoryLazyMaterializingRows : public ILazyMaterializingRows
{
    /// Filled by `filterRangesAndFillRows`: the sorted global row indexes of the rows to read.
    PaddedPODArray<UInt64> rows;

    void filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes) override { rows.assign(sorted_indexes); }
};

using MemoryLazyMaterializingRowsPtr = std::shared_ptr<MemoryLazyMaterializingRows>;

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

    /// The key of the query condition cache for the filter of the query pushed down to this step,
    /// or std::nullopt if the cache cannot be used for it. See `updateQueryConditionCache`.
    std::optional<UInt64> getFilterConditionHashForQueryConditionCache() const;

    /// Lazy materialization (see `optimizeLazyMaterialization2`).
    bool canUseLazyMaterialization() const;
    /// Removes the columns that are not in `required_names` and are not needed by the in-source filters
    /// from the read, makes the read produce the `__global_row_index` column, and returns a step that
    /// reads the removed columns for the given rows. Returns nullptr if there is nothing to remove.
    std::unique_ptr<LazilyReadFromMemoryStorage> keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names);

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

    /// Whether the read produces the `__global_row_index` column for lazy materialization.
    bool read_global_row_index = false;

    void rebuildOutputHeader();

    /// In-source filtering (TopN threshold, row-level security filter, PREWHERE),
    /// or nullptr when there is nothing to apply.
    MemorySourceFilterPtr makeSourceFilter(const NamesAndTypesList & physical_columns) const;

    Pipe makePipe();
};

/// The lazy branch of lazy materialization for the `Memory` storage: reads the deferred columns
/// for exactly the rows that survived the LIMIT, in the order of their global row index.
class LazilyReadFromMemoryStorage final : public ISourceStep
{
public:
    LazilyReadFromMemoryStorage(SharedHeader header, NamesAndTypesList columns_, StorageSnapshotPtr storage_snapshot_, size_t num_streams_);

    void setLazyMaterializingRows(MemoryLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazilyReadFromMemoryStorage"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    NamesAndTypesList columns;
    StorageSnapshotPtr storage_snapshot;
    size_t num_streams;
    MemoryLazyMaterializingRowsPtr lazy_materializing_rows;
};

}
