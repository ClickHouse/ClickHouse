#pragma once
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>

class SipHash;

namespace DB
{

namespace QueryPlanOptimizations
{

/// Main functions which optimize QueryPlan tree.
/// First pass (ideally) apply local idempotent operations on top of Plan.
void optimizeTreeFirstPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes);
/// Second pass is used to apply read-in-order and attach a predicate to PK.
void optimizeTreeSecondPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes, QueryPlan & query_plan);
/// Third pass is used to apply filters such as key conditions and skip indexes to the storages that support them.
/// After that it add CreateSetsStep for the subqueries that has not be used in the filters.
void addStepsToBuildSets(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes);

/// Optimization (first pass) is a function applied to QueryPlan::Node.
/// It can read and update subtree of specified node.
/// It return the number of updated layers of subtree if some change happened.
/// It must guarantee that the structure of tree is correct.
///
/// New nodes should be added to QueryPlan::Nodes list.
/// It is not needed to remove old nodes from the list.
struct Optimization
{
    struct ExtraSettings
    {
        size_t max_step_description_length;

        /// Vector-search-related settings
        size_t max_limit_for_vector_search_queries;
        bool vector_search_with_rescoring;
        VectorSearchFilterStrategy vector_search_filter_strategy;

        /// Other settings
        size_t use_index_for_in_with_subqueries_max_values;
        SizeLimits network_transfer_limits;
    };

    using Function = size_t (*)(QueryPlan::Node *, QueryPlan::Nodes &, const ExtraSettings &);
    const Function apply = nullptr;
    const char * name = "";
    const bool QueryPlanOptimizationSettings::* const is_enabled{};
};

/// Move ARRAY JOIN up if possible
size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Move LimitStep down if possible
size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
size_t trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Replace chain `ExpressionStep -> ExpressionStep` to single ExpressionStep
/// Replace chain `FilterStep -> ExpressionStep` to single FilterStep
size_t tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// Replace chain `FilterStep -> FilterStep` to single FilterStep
/// Note: this breaks short-circuit logic, so it is disabled for now.
size_t tryMergeFilters(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// Move FilterStep down if possible.
/// May split FilterStep and push down only part of it.
size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Convert OUTER JOIN to INNER JOIN if filter after JOIN always filters default values
size_t tryConvertOuterJoinToInnerJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Convert ANY JOIN to SEMI or ANTI JOIN if filter after JOIN always evaluates to false for not-matched or matched rows
size_t tryConvertAnyJoinToSemiOrAntiJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Merge filter into JOIN step and convert CROSS JOIN to INNER.
size_t tryMergeFilterIntoJoinCondition(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Move ExpressionStep after SortingStep if possible.
/// May split ExpressionStep and lift up only a part of it.
size_t tryExecuteFunctionsAfterSorting(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Utilize storage sorting when sorting for window functions.
/// Update information about prefix sort description in SortingStep.
size_t tryReuseStorageOrderingForWindowFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Remove redundant sorting
void tryRemoveRedundantSorting(QueryPlan::Node * root);

/// Remove redundant distinct steps
size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Extract limit and reference vector for vector similarity index
size_t tryUseVectorSearch(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Convert join to subquery with IN if output columns tied to only one table
size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Put some steps under union, so that plan optimization could be applied to union parts separately.
/// For example, the plan can be rewritten like:
///                      - Something -                    - Expression - Something -
/// - Expression - Union - Something -     =>     - Union - Expression - Something -
///                      - Something -                    - Expression - Something -
size_t tryLiftUpUnion(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// Build BloomFilter from right side of JOIN and add condition that looks up into this BloomFilter to the left side of the JOIN.
/// This condition can potentially be pushed down all the way to the storage and filter unmatched rows very early.
bool tryAddJoinRuntimeFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 16> optimizations = {{
        {tryLiftUpArrayJoin, "liftUpArrayJoin", &QueryPlanOptimizationSettings::lift_up_array_join},
        {tryPushDownLimit, "pushDownLimit", &QueryPlanOptimizationSettings::push_down_limit},
        {trySplitFilter, "splitFilter", &QueryPlanOptimizationSettings::split_filter},
        {tryMergeExpressions, "mergeExpressions", &QueryPlanOptimizationSettings::merge_expressions},
        {tryMergeFilters, "mergeFilters", &QueryPlanOptimizationSettings::merge_filters},
        {tryPushDownFilter, "pushDownFilter", &QueryPlanOptimizationSettings::filter_push_down},
        {tryConvertOuterJoinToInnerJoin, "convertOuterJoinToInnerJoin", &QueryPlanOptimizationSettings::convert_outer_join_to_inner_join},
        {tryExecuteFunctionsAfterSorting, "liftUpFunctions", &QueryPlanOptimizationSettings::execute_functions_after_sorting},
        {tryReuseStorageOrderingForWindowFunctions, "reuseStorageOrderingForWindowFunctions", &QueryPlanOptimizationSettings::reuse_storage_ordering_for_window_functions},
        {tryLiftUpUnion, "liftUpUnion", &QueryPlanOptimizationSettings::lift_up_union},
        {tryAggregatePartitionsIndependently, "aggregatePartitionsIndependently", &QueryPlanOptimizationSettings::aggregate_partitions_independently},
        {tryRemoveRedundantDistinct, "removeRedundantDistinct", &QueryPlanOptimizationSettings::remove_redundant_distinct},
        {tryUseVectorSearch, "useVectorSearch", &QueryPlanOptimizationSettings::try_use_vector_search},
        {tryConvertJoinToIn, "convertJoinToIn", &QueryPlanOptimizationSettings::convert_join_to_in},
        {tryMergeFilterIntoJoinCondition, "mergeFilterIntoJoinCondition", &QueryPlanOptimizationSettings::merge_filter_into_join_condition},
        {tryConvertAnyJoinToSemiOrAntiJoin, "convertAnyJoinToSemiOrAntiJoin", &QueryPlanOptimizationSettings::convert_any_join_to_semi_or_anti_join},
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
void optimizePrimaryKeyConditionAndLimit(const Stack & stack);
void optimizeDirectReadFromTextIndex(const Stack & stack, QueryPlan::Nodes & nodes);
void optimizePrewhere(QueryPlan::Node & parent_node);
void optimizeReadInOrder(QueryPlan::Node & node, QueryPlan::Nodes & nodes);
void optimizeAggregationInOrder(QueryPlan::Node & node, QueryPlan::Nodes &);
bool optimizeLazyMaterialization(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, size_t max_limit_for_lazy_materialization);
bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeJoinByShards(QueryPlan::Node & root);
void optimizeDistinctInOrder(QueryPlan::Node & node, QueryPlan::Nodes &);
void updateQueryConditionCache(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings);
bool optimizeVectorSearchSecondPass(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);
void materializeQueryPlanReferences(QueryPlan::Node & node, QueryPlan::Nodes & nodes);
void optimizeUnusedCommonSubplans(QueryPlan::Node & node);

// Should be called once the query plan tree structure is finalized, i.e. no nodes addition, deletion or pushing down should happen after that call.
// Since those hashes are used for join optimization, the calculation performed before join optimization.
std::unordered_map<const QueryPlan::Node *, UInt64> calculateHashTableCacheKeys(const QueryPlan::Node & root);

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes &,
    const QueryPlanOptimizationSettings & optimization_settings);

void optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);

/// A separate tree traverse to apply sorting properties after *InOrder optimizations.
void applyOrder(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root);

/// Returns the name of used projection or nullopt if no projection is used.
std::optional<String> optimizeUseAggregateProjections(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    bool allow_implicit_projections,
    bool is_parallel_replicas_initiator_with_projection_support,
    size_t max_step_description_length);

std::optional<String> optimizeUseNormalProjections(
    Stack & stack,
    QueryPlan::Nodes & nodes,
    bool is_parallel_replicas_initiator_with_projection_support,
    size_t max_step_description_length);

bool addPlansForSets(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & plan, QueryPlan::Node & node, QueryPlan::Nodes & nodes);

/// Enable memory bound merging of aggregation states for remote queries
/// in case it was enabled for local plan
void enableMemoryBoundMerging(QueryPlan::Node & node);

}

}
