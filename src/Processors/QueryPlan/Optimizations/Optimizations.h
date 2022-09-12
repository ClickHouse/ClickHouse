#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>

namespace DB
{

namespace QueryPlanOptimizations
{

/// This is the main function which optimizes the whole QueryPlan tree.
void optimizeTree(const QueryPlanOptimizationSettings & settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes);

/// Optimization is a function applied to QueryPlan::Node.
/// It can read and update subtree of specified node.
/// It return the number of updated layers of subtree if some change happened.
/// It must guarantee that the structure of tree is correct.
///
/// New nodes should be added to QueryPlan::Nodes list.
/// It is not needed to remove old nodes from the list.
struct Optimization
{
    using Function = size_t (*)(QueryPlan::Node *, QueryPlan::Nodes &);
    const Function apply = nullptr;
    const char * name = "";
    const bool QueryPlanOptimizationSettings::* const is_enabled{};
};

/// Move ARRAY JOIN up if possible.
size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Move LimitStep down if possible.
size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes &);

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
size_t trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes);

/// Replace chain `ExpressionStep -> ExpressionStep` to single ExpressionStep
/// Replace chain `FilterStep -> ExpressionStep` to single FilterStep
size_t tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &);

/// Move FilterStep down if possible.
/// May split FilterStep and push down only part of it.
size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Move ExpressionStep after SortingStep if possible.
/// May split ExpressionStep and lift up only a part of it.
size_t tryExecuteFunctionsAfterSorting(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Utilize storage sorting when sorting for window functions.
/// Update information about prefix sort description in SortingStep.
size_t tryReuseStorageOrderingForWindowFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 7> optimizations = {{
        {tryLiftUpArrayJoin, "liftUpArrayJoin", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownLimit, "pushDownLimit", &QueryPlanOptimizationSettings::optimize_plan},
        {trySplitFilter, "splitFilter", &QueryPlanOptimizationSettings::optimize_plan},
        {tryMergeExpressions, "mergeExpressions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownFilter, "pushDownFilter", &QueryPlanOptimizationSettings::filter_push_down},
        {tryExecuteFunctionsAfterSorting, "liftUpFunctions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryReuseStorageOrderingForWindowFunctions, "reuseStorageOrderingForWindowFunctions", &QueryPlanOptimizationSettings::optimize_plan}
    }};

    return optimizations;
}

}

}
