#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>

namespace DB
{

namespace QueryPlanOptimizations
{

/// Main functions which optimize QueryPlan tree.
/// First pass (ideally) apply local idempotent operations on top of Plan.
void optimizeTreeFirstPass(const QueryPlanOptimizationSettings & settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes);
/// Second pass is used to apply read-in-order and attach a predicate to PK.
void optimizeTreeSecondPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes);

/// Optimization (first pass) is a function applied to QueryPlan::Node.
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

/// Move ARRAY JOIN up if possible
size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Move LimitStep down if possible
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

/// Reading in order from MergeTree table if DISTINCT columns match or form a prefix of MergeTree sorting key
size_t tryDistinctReadInOrder(QueryPlan::Node * node);

/// Remove redundant sorting
void tryRemoveRedundantSorting(QueryPlan::Node * root);

/// Remove redundant distinct steps
size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Put some steps under union, so that plan optimisation could be applied to union parts separately.
/// For example, the plan can be rewritten like:
///                      - Something -                    - Expression - Something -
/// - Expression - Union - Something -     =>     - Union - Expression - Something -
///                      - Something -                    - Expression - Something -
size_t tryLiftUpUnion(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 10> optimizations = {{
        {tryLiftUpArrayJoin, "liftUpArrayJoin", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownLimit, "pushDownLimit", &QueryPlanOptimizationSettings::optimize_plan},
        {trySplitFilter, "splitFilter", &QueryPlanOptimizationSettings::optimize_plan},
        {tryMergeExpressions, "mergeExpressions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryPushDownFilter, "pushDownFilter", &QueryPlanOptimizationSettings::filter_push_down},
        {tryExecuteFunctionsAfterSorting, "liftUpFunctions", &QueryPlanOptimizationSettings::optimize_plan},
        {tryReuseStorageOrderingForWindowFunctions,
         "reuseStorageOrderingForWindowFunctions",
         &QueryPlanOptimizationSettings::optimize_plan},
        {tryLiftUpUnion, "liftUpUnion", &QueryPlanOptimizationSettings::optimize_plan},
        {tryAggregatePartitionsIndependently,
         "aggregatePartitionsIndependently",
         &QueryPlanOptimizationSettings::aggregate_partitions_independently},
        {tryRemoveRedundantDistinct, "removeRedundantDistinct", &QueryPlanOptimizationSettings::remove_redundant_distinct},
    }};

    return optimizations;
}

struct Frame
{
    QueryPlan::Node * node = nullptr;
    size_t next_child = 0;
};

using Stack = std::vector<Frame>;

/// Second pass optimizations
void optimizePrimaryKeyCondition(const Stack & stack);
void optimizePrewhere(Stack & stack, QueryPlan::Nodes & nodes);
void optimizeReadInOrder(QueryPlan::Node & node, QueryPlan::Nodes & nodes);
void optimizeAggregationInOrder(QueryPlan::Node & node, QueryPlan::Nodes &);
bool optimizeUseAggregateProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes);
bool optimizeUseNormalProjections(Stack & stack, QueryPlan::Nodes & nodes);

/// Enable memory bound merging of aggregation states for remote queries
/// in case it was enabled for local plan
void enableMemoryBoundMerging(QueryPlan::Node & node, QueryPlan::Nodes &);

}

}
