#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

struct FormatTopKFilterInfo;

class DefaultExpressionsSets;
using DefaultExpressionsSetsPtr = std::shared_ptr<DefaultExpressionsSets>;
using FormatTopKFilterInfoPtr = std::shared_ptr<const FormatTopKFilterInfo>;

class SourceStepWithFilterBase : public ISourceStep
{
public:
    using Base = ISourceStep;
    using Base::Base;

    explicit SourceStepWithFilterBase(SharedHeader output_header_)
        : ISourceStep(std::move(output_header_))
    {
    }

    SourceStepWithFilterBase(const SourceStepWithFilterBase & other)
        : ISourceStep(other)
        , filter_nodes()
        , filter_dags()
        , limit(other.limit)
        , filter_actions_dag()
        , shared_default_expressions_sets(other.shared_default_expressions_sets)
    {
        filter_dags.reserve(other.filter_dags.size());
        filter_nodes.nodes.reserve(other.filter_dags.size());

        for (size_t i = 0; i < other.filter_dags.size(); ++i)
        {
            filter_dags.push_back(other.filter_dags[i].clone());
            filter_nodes.nodes.push_back(&filter_dags.back().findInOutputs(other.filter_nodes.nodes[i]->result_name));
        }

        if (other.filter_actions_dag)
            filter_actions_dag = std::make_shared<const ActionsDAG>(other.filter_actions_dag->clone());
    }
    SourceStepWithFilterBase(SourceStepWithFilterBase &&) = default;

    void addFilter(ActionsDAG filter_dag, std::string column_name)
    {
        filter_nodes.nodes.push_back(&filter_dag.findInOutputs(column_name));
        filter_dags.push_back(std::move(filter_dag));
    }

    void setLimit(size_t limit_value)
    {
        if (limit)
            limit_value = std::min(limit_value, *limit);

        limit = limit_value;
    }

    /// Apply filters that can optimize reading from storage.
    void applyFilters()
    {
        applyFilters(std::move(filter_nodes));
        filter_dags.clear();
    }

    virtual void applyFilters(ActionDAGNodes added_filter_nodes);
    virtual FilterDAGInfoPtr getRowLevelFilter() const { return nullptr; }
    virtual PrewhereInfoPtr getPrewhereInfo() const { return nullptr; }

    /// TopN dynamic filtering (`tryOptimizeTopK`): a source that can filter rows and skip data
    /// using the running threshold of the query's top-K heap (e.g. a Parquet reader) opts in by
    /// overriding both. `supportsTopKDynamicFilter` receives the resolved sort column (in terms of
    /// this step's output header) and must return true only if the source itself physically reads
    /// that column with that type - not when it is appended after the read (a virtual column such
    /// as `_path`, or a Hive partition column taken from the file path), since the reader could
    /// not evaluate the threshold against it. `column_name` in the info is that column's name.
    virtual bool supportsTopKDynamicFilter(const ColumnWithTypeAndName & /*sort_column*/) const { return false; }
    virtual void setTopKFilter(std::shared_ptr<const FormatTopKFilterInfo> /*info*/) {}
    /// True only if the source honors `FormatTopKFilterInfo::filter_rows = false` (no per-row filter).
    virtual bool supportsTopKStatisticsOnlyFilter() const { return false; }

    /// The whole output is consumed only by `min` / `max` (see `FormatFilterInfo::min_max_from_statistics`).
    virtual bool supportsMinMaxFromStatistics() const { return false; }
    virtual void setMinMaxFromStatistics() {}

    /// Make the readers of this step share the sets of the column defaults that read a table (`x IN table`) with the
    /// other reading steps of the same logical read, such as the children of a `Merge` read, so that each set is built
    /// once for it, from one snapshot of its table. Without it, the step builds the sets for its own readers.
    virtual void setDefaultExpressionsSets(DefaultExpressionsSetsPtr sets) { shared_default_expressions_sets = std::move(sets); }

    const std::shared_ptr<const ActionsDAG> & getFilterActionsDAG() const { return filter_actions_dag; }
    std::shared_ptr<const ActionsDAG> detachFilterActionsDAG() { return std::move(filter_actions_dag); }

    bool hasCorrelatedExpressions() const override
    {
        if (filter_actions_dag)
            return filter_actions_dag->hasCorrelatedColumns();
        return false;
    }

private:
    /// Will be cleared after applyFilters() is called.
    ActionDAGNodes filter_nodes;
    VectorWithMemoryTracking<ActionsDAG> filter_dags;

protected:
    std::optional<size_t> limit;
    std::shared_ptr<const ActionsDAG> filter_actions_dag;
    /// Set by `setDefaultExpressionsSets`.
    DefaultExpressionsSetsPtr shared_default_expressions_sets;
};

/** Source step that can use filters and limit for more efficient pipeline initialization.
  * Filters must be added before pipeline initialization.
  * Limit must be set before pipeline initialization.
  */
class SourceStepWithFilter : public SourceStepWithFilterBase
{
public:
    using Base = SourceStepWithFilterBase;
    using Base::Base;

    SourceStepWithFilter(
        SharedHeader output_header_,
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_)
        : SourceStepWithFilterBase(std::move(output_header_))
        , required_source_columns(column_names_)
        , query_info(query_info_)
        , storage_snapshot(storage_snapshot_)
        , context(context_)
    {
    }

    SourceStepWithFilter(const SourceStepWithFilter &) = default;

    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    void setStorageLimits(std::shared_ptr<const StorageLimitsList> storage_limits) { query_info.storage_limits = std::move(storage_limits); }
    FilterDAGInfoPtr getRowLevelFilter() const override { return query_info.row_level_filter; }
    PrewhereInfoPtr getPrewhereInfo() const override { return query_info.prewhere_info; }
    ContextPtr getContext() const { return context; }
    const StorageSnapshotPtr & getStorageSnapshot() const { return storage_snapshot; }

    bool isQueryWithFinal() const { return query_info.isFinal(); }

    const Names & requiredSourceColumns() const { return required_source_columns; }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    virtual void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value);

    /// Whether updatePrewhereInfo() may be invoked more than once on this step.
    /// The base implementation rebuilds the output header from the full sample block, so it is
    /// idempotent. Format-based steps (file/url/object storage) shrink their header incrementally
    /// and can only accept a single prewhere update, so they return false to opt out of the
    /// optimize_prewhere_after_pushdown second pass.
    virtual bool canUpdatePrewhereInfoMultipleTimes() const { return true; }

    void describeActions(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    static Block applyPrewhereActions(Block block, const FilterDAGInfoPtr & row_level_filter, const PrewhereInfoPtr & prewhere_info);

protected:
    Names required_source_columns;
    SelectQueryInfo query_info;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
};

}
