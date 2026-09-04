#pragma once
#include <Core/Joins.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <array>
#include <unordered_map>

class SipHash;

namespace DB
{

class JoinStepLogical;

class FutureSetFromSubquery;
using FutureSetFromSubqueryPtr = std::shared_ptr<FutureSetFromSubquery>;

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
        size_t max_step_description_length{};

        /// Vector-search-related settings
        size_t max_limit_for_vector_search_queries{};
        bool vector_search_with_rescoring{};
        VectorSearchFilterStrategy vector_search_filter_strategy{};

        /// Other settings
        size_t use_index_for_in_with_subqueries_max_values{};
        SizeLimits network_transfer_limits;
        bool optimize_prewhere{};
        bool remove_unused_columns{};

        bool use_skip_indexes_for_top_k{};
        bool use_top_k_dynamic_filtering{};
        bool use_top_k_dynamic_filtering_for_variable_length_types{};
        size_t max_limit_for_top_k_optimization{};
        bool use_skip_indexes_on_data_read{};
        bool read_in_order{};
        bool read_in_order_through_join{};

        /// Mirrors `QueryPlanOptimizationSettings::join_swap_table`. `std::nullopt` means
        /// "auto" (swap decided by `optimizeJoinLegacy` from per-side row estimations);
        /// `true`/`false` are explicit. `topKThroughJoin` consults it because deferring to
        /// the second-pass read-in-order would silently disable both optimizations if the
        /// join is swapped from `LEFT` to `RIGHT` after we returned.
        std::optional<bool> join_swap_table;

        bool enable_group_by_top_k_optimization{};
        UInt64 top_k_optimization_observation_rows{};
        bool is_explain{};

        size_t max_block_size{};

        // parallel replicas
        bool parallel_replicas_filter_pushdown = false;

        /// Mirrors `QueryPlanOptimizationSettings::push_down_volume_reducing_functions`.
        /// `tryExecuteFunctionsAfterSorting` consults it to avoid pinging volume-reducing
        /// functions back above a `SortingStep` that `tryPushDownVolumeReducingFunction`
        /// pushed below it.
        bool push_down_volume_reducing_functions = false;
        /// Top-K optimizations rely on a runtime `TopKThresholdTracker` shared between
        /// `SortingStep` and `ReadFromMergeTree`, and the dynamic-filtering path adds
        /// an internal `__topKFilter` function that is not registered in `FunctionFactory`.
        /// Neither can survive serialization to remote workers, so we suppress the
        /// optimization when the plan is going to be distributed or serialized.
        bool make_distributed_plan = false;
        bool serialize_query_plan = false;
        /// When short-circuit is off, a FilterStep still masks a throwing atom by splitting the AND into
        /// sequential filters. fuseFilterIntoArrayJoin can't reproduce that, so it won't fuse a multi-atom
        /// AND in this mode.
        bool short_circuit_function_evaluation_disabled = false;
        bool lower_array_join_function = false;
        bool enable_lazy_columns_replication = false;
    };

    using Function = size_t (*)(QueryPlan::Node *, QueryPlan::Nodes &, const ExtraSettings &);
    const Function apply = nullptr;
    const char * name = "";
    const bool QueryPlanOptimizationSettings::* const is_enabled{};
};

/// Move ARRAY JOIN up if possible
size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Lower an `arrayJoin` function inside an Expression/Filter into a real ArrayJoinStep.
size_t tryLowerArrayJoinFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Move LimitStep down if possible
size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// When an aggregation feeds ORDER BY <its lone count() output> LIMIT n, let every two-level
/// bucket of the aggregation output materialize only its n best groups by that count.
size_t tryPushBucketTopKIntoAggregation(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

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

/// Fuse a FilterStep directly above an ArrayJoinStep: element-only conjuncts become the ArrayJoinStep's
/// element filter, applied before expansion so filtered elements are never expanded or replicated.
size_t tryFuseFilterIntoArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);
/// Move volume-reducing functions down if possible.
size_t tryPushDownVolumeReducingFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Volume-reducing function nodes (`length`, `lengthUTF8`, `empty`, `notEmpty`) of `actions`,
/// grouped by their argument node and restricted to arguments that nothing else in the DAG needs.
/// For such an argument, computing the functions *replaces* the wide column instead of adding to
/// it, which is what `tryPushDownVolumeReducingFunction` requires to be worth doing.
/// `tryExecuteFunctionsAfterSorting` uses the same set to avoid lifting those functions back above
/// a `SortingStep` they have been pushed below.
std::unordered_map<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs>
collectVolumeReducingFunctionsReplacingTheirArgument(const ActionsDAG & actions);

/// Volume-reducing function nodes of `actions` (and their aliases among the outputs) that must stay
/// in the lower part when the DAG is split in two by `trySplitFilter` or
/// `tryExecuteFunctionsAfterSorting`. Lifting such a function would make its wide argument cross
/// the step again, undoing `tryPushDownVolumeReducingFunction` and re-triggering it, so the three
/// optimizations would move the same nodes in opposite directions forever. This is a wider set than
/// `collectVolumeReducingFunctionsReplacingTheirArgument`: after `tryMergeExpressions` merges the
/// pushed step into its neighbor, the argument may be a computed column or have other readers
/// (a `Filter` condition), yet the function still has to stay below. Functions whose argument is
/// surfaced by the DAG anyway are not collected — the wide column crosses the step regardless, so
/// lifting them loses nothing. `low_part_root` identifies nodes that a caller already keeps below
/// its barrier (for example, a `FilterStep` predicate) and which therefore do not make the
/// argument flow through that barrier.
std::unordered_set<const ActionsDAG::Node *> collectVolumeReducingFunctionsToKeepBelow(
    const ActionsDAG & actions, const ActionsDAG::Node * low_part_root = nullptr);

/// Convert OUTER JOIN to INNER JOIN if filter after JOIN always filters default values
size_t tryConvertOuterJoinToInnerJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Short-circuit a JOIN whose ON condition folds to a constant false: replace each input side that
/// cannot contribute a row (both sides for INNER/CROSS/SEMI, the non-preserved side for LEFT/RIGHT)
/// with an empty source, so the non-contributing side is not read. The JoinStep is kept in place so
/// join validation still runs.
size_t tryShortCircuitConstantFalseJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

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
size_t tryUseVectorSearchWithVectorIndexFirstPass(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Convert join to subquery with IN if output columns tied to only one table
size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Put some steps under union, so that plan optimization could be applied to union parts separately.
/// For example, the plan can be rewritten like:
///                      - Something -                    - Expression - Something -
/// - Expression - Union - Something -     =>     - Union - Expression - Something -
///                      - Something -                    - Expression - Something -
size_t tryLiftUpUnion(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);

/// Removes unused columns from the query plan. Unused columns can appear after other optimizations, such as filter
/// push down over JOINs. If a column is only used for filtering after a JOIN, and the filter is pushed down into
/// the JOIN condition, then the column may become unused in the plan.
/// This optimization traverses the query plan and attempts to remove such unused columns from the steps if they
/// support the optimization (canRemoveUnusedColumns method).
/// It might happen that a child step supports removing unused columns, but it cannot remove any more columns
/// (canRemoveColumnsFromOutput method returns false, e.g. JoinStepLogical always needs to keep at least one column for
/// its output). In this case or when the children step doesn't support the optimization at all, then the inputs of the
/// optimized step doesn't change.
/// If the children support the optimization but cannot produce the expected output (e.g. JoinStepLogical can remove
/// arbitrary number of columns as long as at least one column remains in the output), then the optimization adds an
/// expression step to convert between the child's new output and the input of the parent node.
size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &);

/// Build BloomFilter from right side of JOIN and add condition that looks up into this BloomFilter to the left side of the JOIN.
/// This condition can potentially be pushed down all the way to the storage and filter unmatched rows very early.
bool tryAddJoinRuntimeFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

/// Try to prune LHS table granules using JoinRuntimeFilter & index analysis
void registerLeftSideIndexAnalysisSecondPass(QueryPlan::Node & node, const QueryPlanOptimizationSettings & optimization_settings);

/// Optimize ORDER BY ... LIMIT n query by using skip index or Prewhere threshold filtering
size_t tryOptimizeTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// Push LIMIT into GROUP BY via bounded heap when GROUP BY matches or is a prefix of ORDER BY keys
size_t tryOptimizeGroupByTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// Push ORDER BY ... LIMIT n down through a Join when the sort key only references
/// columns from the side preserved by the join (LEFT/RIGHT). Restricts how many rows
/// the preserved-side input must produce before joining.
size_t tryTopKThroughJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 23> optimizations = {{
        /// Run first, before splitFilter/pushDownFilter/mergeFilterIntoJoinCondition, so the
        /// constant-false ON condition is still intact on the JoinStepLogical (those passes would
        /// otherwise lower it into a CROSS + Filter on one input and hide it from this optimization).
        {tryShortCircuitConstantFalseJoin,
         "shortCircuitConstantFalseJoin",
         &QueryPlanOptimizationSettings::short_circuit_constant_false_join},
        {tryLowerArrayJoinFunction, "lowerArrayJoinFunction", &QueryPlanOptimizationSettings::lower_array_join_function},
        {tryLiftUpArrayJoin, "liftUpArrayJoin", &QueryPlanOptimizationSettings::lift_up_array_join},
        {tryPushDownLimit, "pushDownLimit", &QueryPlanOptimizationSettings::push_down_limit},
        {tryPushBucketTopKIntoAggregation, "aggregationBucketTopK", &QueryPlanOptimizationSettings::aggregation_bucket_top_k},
        {trySplitFilter, "splitFilter", &QueryPlanOptimizationSettings::split_filter},
        {tryMergeExpressions, "mergeExpressions", &QueryPlanOptimizationSettings::merge_expressions},
        {tryMergeFilters, "mergeFilters", &QueryPlanOptimizationSettings::merge_filters},
        {tryPushDownFilter, "pushDownFilter", &QueryPlanOptimizationSettings::filter_push_down},
        {tryFuseFilterIntoArrayJoin, "fuseFilterIntoArrayJoin", &QueryPlanOptimizationSettings::fuse_filter_into_array_join},
        {tryConvertOuterJoinToInnerJoin, "convertOuterJoinToInnerJoin", &QueryPlanOptimizationSettings::convert_outer_join_to_inner_join},
        {tryPushDownVolumeReducingFunction, "pushDownVolumeReducingFunction", &QueryPlanOptimizationSettings::push_down_volume_reducing_functions},
        {tryExecuteFunctionsAfterSorting, "liftUpFunctions", &QueryPlanOptimizationSettings::execute_functions_after_sorting},
        {tryReuseStorageOrderingForWindowFunctions,
         "reuseStorageOrderingForWindowFunctions",
         &QueryPlanOptimizationSettings::reuse_storage_ordering_for_window_functions},
        {tryLiftUpUnion, "liftUpUnion", &QueryPlanOptimizationSettings::lift_up_union},
        {tryRemoveRedundantDistinct, "removeRedundantDistinct", &QueryPlanOptimizationSettings::remove_redundant_distinct},
        {tryUseVectorSearchWithVectorIndexFirstPass, "useVectorSearch", &QueryPlanOptimizationSettings::try_use_vector_search},
        {tryConvertJoinToIn, "convertJoinToIn", &QueryPlanOptimizationSettings::convert_join_to_in},
        {tryMergeFilterIntoJoinCondition, "mergeFilterIntoJoinCondition", &QueryPlanOptimizationSettings::merge_filter_into_join_condition},
        {tryConvertAnyJoinToSemiOrAntiJoin,
         "convertAnyJoinToSemiOrAntiJoin",
         &QueryPlanOptimizationSettings::convert_any_join_to_semi_or_anti_join},
        {tryRemoveUnusedColumns, "removeUnusedColumns", &QueryPlanOptimizationSettings::remove_unused_columns},
        {tryOptimizeTopK, "tryOptimizeTopK", &QueryPlanOptimizationSettings::try_use_top_k_optimization},
        {tryTopKThroughJoin, "topKThroughJoin", &QueryPlanOptimizationSettings::top_k_through_join},
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
void processAndOptimizeTextIndexFunctions(const Stack & stack, QueryPlan::Nodes & nodes, bool direct_read_from_text_index);
void optimizeReadInOrder(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
void optimizePrewhere(QueryPlan::Node & parent_node, bool remove_unused_columns, bool suppress_for_vector_search = true);
void optimizeAggregationInOrder(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
bool optimizeLazyMaterialization2(QueryPlan::Node & root, QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings, size_t max_limit_for_lazy_materialization);
void optimizeLazyFinal(const Stack & stack, QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeJoinByShards(QueryPlan::Node & root);
void optimizeParallelFullSortingMergeJoin(QueryPlan::Node & root, size_t num_shards);
void optimizeDistinctInOrder(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeLimitForAggregationInOrder(QueryPlan::Node & root);
void optimizeLimitByInOrder(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void pushLimitByIntoSort(QueryPlan::Node & node);
void optimizeAggregationPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeLimitByPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeDistinctPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeWindowPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void optimizeCreatingSetPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);
void updateQueryConditionCache(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings);
bool optimizeVectorSearchWithVectorIndexSecondPass(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &);
bool optimizeVectorSearchWithQuantizedCodes(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings, size_t max_limit_for_lazy_materialization);
/// Replaces a `CommonSubplanReferenceStep` with a clone of the subplan it references. The subplan's
/// IN-subquery sets are appended to `extracted_sets` and removed from the plan, because a
/// `FutureSetFromSubquery` source can be claimed only once; the caller attaches one builder for them
/// above a node that dominates every copy.
void materializeQueryPlanReferences(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, std::vector<FutureSetFromSubqueryPtr> & extracted_sets);
void optimizeUnusedCommonSubplans(QueryPlan::Node & node);
void useMemoryBufferForCommonSubplanResult(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings);
void optimizeJoinLazyIndexing(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);

// Should be called once the query plan tree structure is finalized, i.e. no nodes addition, deletion or pushing down should happen after that call.
// Since those hashes are used for join optimization, the calculation performed before join optimization.
std::unordered_map<const QueryPlan::Node *, UInt64> calculateHashTableCacheKeys(const QueryPlan::Node & root);

/// Stamp every AggregatingStep in the plan with a hash-table preallocation cache key derived from
/// the query plan (the node's bottom-up hash from calculateHashTableCacheKeys), instead of from the
/// AST. Mirrors how join steps get their keys. No-op unless collect_hash_table_stats_during_aggregation.
void setAggregationHashTableCacheKeys(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root);

/// Populates two maps in lock-step:
///   raw_hashes[N]  = bottom-up hash of the sub-plan rooted at N, independent of N's parent.
///   cache_keys[N]  = raw_hashes[N] XOR (the per-side contribution of N's parent join step).
/// `raw_hashes` is what the join reorder pass needs to derive cache keys for sub-join nodes
/// it builds itself; `cache_keys` matches the value `HashTablesStatistics` is keyed by.
void calculateHashTableCacheKeys(
    const QueryPlan::Node & root,
    std::unordered_map<const QueryPlan::Node *, UInt64> & cache_keys,
    std::unordered_map<const QueryPlan::Node *, UInt64> & raw_hashes);

/// Per-side join-step hash used to derive HashTablesStatistics cache keys after join reorder.
UInt64 calculateJoinStepCacheKeyContribution(const JoinStepLogical & join_step, JoinTableSide side);

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes &,
    const QueryPlanOptimizationSettings & optimization_settings);

void optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings &);

/// A separate tree traverse to apply sorting properties after *InOrder optimizations.
void applyOrder(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root);

/// A separate tree traverse that propagates the stream-disjointness property (no two output streams
/// carry the same key value).
void applyStreamDisjointness(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root);

/// Returns the name of used projection or nullopt if no projection is used.
std::optional<String> optimizeUseAggregateProjections(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings);

std::optional<String> optimizeUseNormalProjections(
    Stack & stack,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings);

/// Returns `COUNT()` query directly from the text index posting metadata.
bool optimizeTrivialCountFromTextIndex(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

bool addPlansForSets(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & plan, QueryPlan::Node & node, QueryPlan::Nodes & nodes);

/// Resolve all DelayedMaterializingCTEsStep nodes in the plan tree.
/// Must be called after the second optimization pass so that is_planned flags
/// set by buildOrderedSetInplace are already visible.
void resolveMaterializingCTEs(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & root_plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes);

/// Enable memory bound merging of aggregation states for remote queries
/// in case it was enabled for local plan
void enableMemoryBoundMerging(QueryPlan::Node & node);

}

}
