#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

/** Source step that can use filters for more efficient pipeline initialization.
  * Filters must be added before pipeline initialization.
  */
class SourceStepWithFilter : public ISourceStep
{
public:
    using Base = ISourceStep;
    using Base::Base;

    SourceStepWithFilter(
        DataStream output_stream_,
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_)
        : ISourceStep(std::move(output_stream_))
        , required_source_columns(column_names_)
        , query_info(query_info_)
        , prewhere_info(query_info.prewhere_info)
        , storage_snapshot(storage_snapshot_)
        , context(context_)
    {
    }

    const ActionsDAGPtr & getFilterActionsDAG() const { return filter_actions_dag; }

    const SelectQueryInfo & getQueryInfo() const { return query_info; }
    const PrewhereInfoPtr & getPrewhereInfo() const { return prewhere_info; }
    ContextPtr getContext() const { return context; }
    const StorageSnapshotPtr & getStorageSnapshot() const { return storage_snapshot; }

    bool isQueryWithFinal() const { return query_info.isFinal(); }

    const Names & requiredSourceColumns() const { return required_source_columns; }

    void addFilter(ActionsDAGPtr filter_dag, std::string column_name)
    {
        filter_nodes.nodes.push_back(&filter_dag->findInOutputs(column_name));
        filter_dags.push_back(std::move(filter_dag));
    }

    void addFilterFromParentStep(const ActionsDAG::Node * filter_node)
    {
        filter_nodes.nodes.push_back(filter_node);
    }

    /// Apply filters that can optimize reading from storage.
    void applyFilters()
    {
        applyFilters(std::move(filter_nodes));
        filter_dags = {};
    }

    virtual void applyFilters(ActionDAGNodes added_filter_nodes);

    virtual void updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value);

    void describeActions(FormatSettings & format_settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    static Block applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info);

protected:
    Names required_source_columns;
    SelectQueryInfo query_info;
    PrewhereInfoPtr prewhere_info;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;

    ActionsDAGPtr filter_actions_dag;

private:
    /// Will be cleared after applyFilters() is called.
    ActionDAGNodes filter_nodes;
    std::vector<ActionsDAGPtr> filter_dags;
};

}
