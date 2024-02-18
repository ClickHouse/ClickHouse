#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/SelectQueryInfo.h>

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

    explicit SourceStepWithFilter(DataStream output_stream_, PrewhereInfoPtr prewhere_info_ = nullptr)
        : ISourceStep(std::move(output_stream_)), prewhere_info(std::move(prewhere_info_))
    {
    }

    const std::vector<ActionsDAGPtr> & getFilters() const
    {
        return filter_dags;
    }

    const ActionDAGNodes & getFilterNodes() const
    {
        return filter_nodes;
    }

    const PrewhereInfoPtr & getPrewhereInfo() const
    {
        return prewhere_info;
    }

    void addFilter(ActionsDAGPtr filter_dag, std::string column_name)
    {
        filter_nodes.nodes.push_back(&filter_dag->findInOutputs(column_name));
        filter_dags.push_back(std::move(filter_dag));
    }

    void addFilter(ActionsDAGPtr filter_dag, const ActionsDAG::Node * filter_node)
    {
        filter_nodes.nodes.push_back(filter_node);
        filter_dags.push_back(std::move(filter_dag));
    }

    /// Apply filters that can optimize reading from storage.
    virtual void applyFilters() {}

protected:
    std::vector<ActionsDAGPtr> filter_dags;
    ActionDAGNodes filter_nodes;
    PrewhereInfoPtr prewhere_info;
};

}
