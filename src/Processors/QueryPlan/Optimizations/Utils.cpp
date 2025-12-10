#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

bool isPassthroughActions(const ActionsDAG & actions_dag)
{
    return actions_dag.getOutputs() == actions_dag.getInputs() && actions_dag.trivial();
}

QueryPlan::Node * makeExpressionNodeOnTopOf(QueryPlan::Node * node, ActionsDAG actions_dag, const String & filter_column_name, QueryPlan::Nodes & nodes)
{
    const auto & header = node->step->getOutputHeader();
    if (!header && !actions_dag.getInputs().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create ExpressionStep on top of node without header, dag: {}",
        actions_dag.dumpDAG());

    if (isPassthroughActions(actions_dag))
        return node;

    QueryPlanStepPtr step;

    if (filter_column_name.empty())
        step = std::make_unique<ExpressionStep>(header, std::move(actions_dag));
    else
        step = std::make_unique<FilterStep>(header, std::move(actions_dag), filter_column_name, false);

    return &nodes.emplace_back(QueryPlan::Node{std::move(step), {node}});
}

}
