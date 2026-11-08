#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/safe_cast.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>

#include <Functions/FunctionsMiscellaneous.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/TableJoin.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/JoinGraphStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/ReadFromCommonBufferStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/JoiningTransform.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <stack>
#include <string>
#include <string_view>
#include <unordered_map>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>
#include <base/types.h>

namespace ProfileEvents
{
    extern const Event JoinOptimizeMicroseconds;
    extern const Event JoinOrderJoinsCostedWithoutRowEstimate;
    extern const Event JoinEnumerationSetPlans;
    extern const Event JoinEnumerationSplits;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_hash_table_stats_for_join_reordering;
}

namespace QueryPlanOptimizations
{

static String dumpStatsForLogs(const RelationStats & stats);

struct RuntimeHashStatisticsContext
{
    /// `HashTablesStatistics` keys identify a specific hash table BUILT from a subtree AND
    /// keyed by specific columns — both pieces are needed: the same right-side subtree
    /// joined on `t2.a` and joined on `t2.b` produces two physically different hash tables
    /// with different sizes/NDVs, so they must NOT share a cache entry. The key encoding is:
    ///     cache_keys[N]  =  raw_hashes[N]  XOR  <per-side contribution of N's parent join>
    /// where the contribution hashes the parent join's equi-key columns on the side N sits on.
    /// Populated for every node by `calculateHashTableCacheKeys`; mutated during join reorder
    /// to reflect the post-reorder parent of each node.
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    /// Bottom-up hash of the subtree rooted at the node — does NOT include any parent-join
    /// contribution, so it identifies "what data" but not "what hash table keyed how". Used
    /// by join-reorder code in `chooseJoinOrder` to derive cache keys for sub-join nodes
    /// built during reorder, where the post-reorder parent's contribution differs from
    /// whatever was originally stamped into `cache_keys` during the pre-reorder walk.
    std::unordered_map<const QueryPlan::Node *, UInt64> raw_hashes;
    StatsCollectingParams params;

    RuntimeHashStatisticsContext(const QueryPlanOptimizationSettings & optimization_settings, const QueryPlan::Node & root_node)
        : params{
            /*key_=*/0,
            /*enable=*/ optimization_settings.collect_hash_table_stats_during_joins,
            optimization_settings.max_entries_for_hash_table_stats,
            optimization_settings.max_size_to_preallocate_for_joins}
    {
        if (optimization_settings.collect_hash_table_stats_during_joins)
        {
            calculateHashTableCacheKeys(root_node, cache_keys, raw_hashes);
        }
    }

    UInt64 getCachedKey(const QueryPlan::Node * node)
    {
        if (auto it = cache_keys.find(node); it != cache_keys.end())
            return it->second;
        return 0;
    }

    /// Gives `to`, a copy of the subplan `from`, the hashes of `from`.
    void copyHashes(const QueryPlan::Node * from, const QueryPlan::Node * to)
    {
        if (auto it = raw_hashes.find(from); it != raw_hashes.end())
            raw_hashes[to] = it->second;
        if (auto it = cache_keys.find(from); it != cache_keys.end())
            cache_keys[to] = it->second;
    }

    UInt64 getRawHash(const QueryPlan::Node * node) const
    {
        if (auto it = raw_hashes.find(node); it != raw_hashes.end())
            return it->second;
        return 0;
    }

    std::optional<size_t> getCachedHint(const QueryPlan::Node * node)
    {
        if (auto cache_key = getCachedKey(node))
        {
            auto & hash_table_stats = getHashTablesStatistics<HashJoinEntry>();
            /// A size recorded under a runtime filter from outside the build side describes that plan only.
            if (auto hint = hash_table_stats.getSizeHint(params.setKey(cache_key)))
                return hint->unfiltered_source_rows;
        }
        return {};
    }

    /// Mirror what `calculateHashTableCacheKeys` would have produced for an equivalent join in
    /// the original tree, but for `new_node` that the join-reorder pass is emitting on top of
    /// `left_child_node` and `right_child_node` (which can themselves be original leaves or
    /// sub-joins built earlier in the same reorder loop). Returns the derived right-side key,
    /// suitable for `JoinStepLogical::setRightHashTableCacheKey`.
    ///
    /// Each child's final cache key combines its parent-independent subtree hash with the
    /// parent join's per-side contribution (see `cache_keys` doc above for why both are
    /// needed). We start from `raw_hashes[child]` rather than the previously-xored value in
    /// `cache_keys[child]`, because under reorder the new parent's contribution can differ
    /// from the original tree's parent contribution that was stamped into `cache_keys`.
    struct DerivedJoinCacheKeys
    {
        UInt64 right_key = 0;
        UInt64 output_key = 0;
    };

    DerivedJoinCacheKeys deriveCacheKeysForNewJoin(
        const QueryPlan::Node * left_child_node,
        const QueryPlan::Node * right_child_node,
        const QueryPlan::Node & new_node,
        const JoinStepLogical & join_step)
    {
        if (cache_keys.empty())
            return {};

        UInt64 raw_left = getRawHash(left_child_node);
        UInt64 raw_right = getRawHash(right_child_node);
        UInt64 left_key = raw_left ^ calculateJoinStepCacheKeyContribution(join_step, JoinTableSide::Left);
        UInt64 right_key = raw_right ^ calculateJoinStepCacheKeyContribution(join_step, JoinTableSide::Right);

        /// Update cache_keys to reflect the new parent for these children (in case any
        /// downstream code reads them back via getCachedKey).
        cache_keys[left_child_node] = left_key;
        cache_keys[right_child_node] = right_key;

        /// Record this join's own raw hash so any outer reorder iteration can derive its key
        /// the same way; cache_keys gets the same value initially and may later be xored when
        /// new_node becomes a child of yet another reorder-built join.
        SipHash node_hash;
        node_hash.update(left_key);
        node_hash.update(right_key);
        UInt64 raw_new = node_hash.get64();
        raw_hashes[&new_node] = raw_new;
        cache_keys[&new_node] = raw_new;

        /// Derive a key for the join output stats that takes the kind, strictness
        /// and non-equi conditions into account, in addition to the equi conditions
        /// covered by `calculateJoinStepCacheKeyContribution`.
        SipHash output_hash;
        output_hash.update(raw_new);
        const auto & join_operator = join_step.getJoinOperator();
        output_hash.update(join_operator.kind);
        output_hash.update(join_operator.strictness);
        for (const auto & condition : join_operator.expression)
        {
            if (condition.isFunction(JoinConditionOperator::Equals) || condition.isFunction(JoinConditionOperator::NullSafeEquals))
                continue;
            condition.getNode()->updateHash(output_hash);
        }

        return {right_key, output_hash.get64()};
    }
};

bool optimizeJoinLegacy(QueryPlan::Node & node, QueryPlan::Nodes & /*nodes*/, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2 || join_step->isOptimized())
        return false;

    const auto & join = join_step->getJoin();
    if (join->pipelineType() != JoinPipelineType::FillRightFirst || !join->isCloneSupported())
        return true;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);

    const auto & table_join = join->getTableJoin();

    /// Algorithms other than HashJoin may not support all JOIN kinds, so changing from LEFT to RIGHT is not always possible
    bool allow_outer_join = typeid_cast<const HashJoin *>(join.get());
    if (table_join.kind() != JoinKind::Inner && !allow_outer_join)
        return true;

    /// fixme: USING clause handled specially in join algorithm, so swap breaks it
    /// fixme: Swapping for SEMI and ANTI joins should be alright, need to try to enable it and test
    if (table_join.hasUsing() || table_join.strictness() != JoinStrictness::All)
        return true;

    bool need_swap = false;
    if (!join_step->swap_join_tables.has_value())
    {
        auto lhs_extimation = estimateReadRowsCount(*node.children[0]).estimated_rows;
        auto rhs_extimation = estimateReadRowsCount(*node.children[1]).estimated_rows;
        LOG_TRACE(getLogger("optimizeJoinLegacy"), "Left table estimation: {}, right table estimation: {}",
            lhs_extimation ? toString(lhs_extimation.value()) : "unknown",
            rhs_extimation ? toString(rhs_extimation.value()) : "unknown");

        if (lhs_extimation && rhs_extimation && lhs_extimation < rhs_extimation)
            need_swap = true;
    }
    else if (join_step->swap_join_tables.value())
    {
        need_swap = true;
    }

    if (!need_swap)
        return true;

    const auto & headers = join_step->getInputHeaders();
    if (headers.size() != 2)
        return true;

    auto left_stream_input_header = headers.front();
    auto right_stream_input_header = headers.back();

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->swapSides();
    auto updated_join = join->clone(updated_table_join, right_stream_input_header, left_stream_input_header);

    /// After swapping, the join output may lose columns because TableJoin::swapSides
    /// swaps result_columns_from_left_table with columns_added_by_join, and the join
    /// algorithm may filter different columns from the (now swapped) left input.
    /// If any column required by downstream steps would be missing, skip the swap.
    auto original_output = join_step->getOutputHeader();
    auto swapped_algorithm_header = JoiningTransform::transformHeader(*right_stream_input_header, updated_join);
    for (const auto & col : *original_output)
    {
        if (!swapped_algorithm_header.has(col.name))
            return true;
    }

    join_step->setJoin(std::move(updated_join), /* swap_streams= */ true);

    return true;
}

bool convertLogicalJoinToPhysical(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    bool keep_logical = optimization_settings.keep_logical_steps;
    /// Distributed plan keeps logical joins steps. They are converted to physical steps afterwards, when plan fragment is executed by a worker.
    keep_logical |= optimization_settings.make_distributed_plan;
    if (keep_logical)
        return false;
    if (!typeid_cast<JoinStepLogical *>(node.step.get()))
        return false;
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    JoinStepLogical::buildPhysicalJoin(node, optimization_settings, nodes);

    return true;
}

struct QueryGraphBuilder
{
    JoinExpressionActions expression_actions;

    std::vector<RelationStats> relation_stats;
    std::vector<QueryPlan::Node *> inputs;

    std::vector<JoinActionRef> join_edges;
    /// See QueryGraph::edge_conflict_ops
    std::vector<size_t> join_edge_ops;

    /// See `QueryGraph::probe_keys_within_preserved_side`.
    bool probe_keys_within_preserved_side = false;

    /// Outer joined relation should be joined after all other relations involved in its join expressions.
    /// It is joined with specified join kind.
    /// The `join_kinds` maps (join relation index) -> (set of relations it depends on, join kind)
    std::unordered_map<size_t, std::pair<BitSet, JoinKind>> join_kinds;
    std::unordered_map<size_t, ActionsDAG::NodeRawConstPtrs> type_changes;
    /// ON-clause predicates of outer joins, see QueryGraph::outer_join_conditions
    std::unordered_map<JoinActionRef, BitSet> outer_join_conditions;
    /// See QueryGraph::null_supplying_subtree_relations and QueryGraph::requires_conflict_detector
    BitSet null_supplying_subtree_relations;
    bool requires_conflict_detector = false;
    /// See QueryGraph::cross_product_links
    std::vector<std::pair<BitSet, BitSet>> cross_product_links;

    /// Whether the disjunction push-down already ran on the joins this graph was built from. Reordering
    /// rebuilds the join steps, and a fresh step would let the push-down run a second time and add the
    /// same partial predicate again - a filter the read already applies through PREWHERE. Only a plan
    /// that is optimized more than once reaches that state (the parallel-replicas plan-based path does).
    bool disjunctions_optimization_applied = false;

    /// One record per binary join operator of the original tree, captured for the optional conflict
    /// detector (CD-A/CD-C). Relation ids are local to this (sub)graph and shifted in `uniteGraphs`.
    /// See QueryGraph::conflict_ops / ConflictJoinOp.
    std::vector<ConflictJoinOp> conflict_ops;

    struct BuilderContext
    {
        const QueryPlanOptimizationSettings & optimization_settings;
        RuntimeHashStatisticsContext statistics_context;
        JoinSettings join_settings;
        SortingStep::Settings sorting_settings;
        String stats_hint;
        UInt64 effective_randomize_seed = 0;
        /// Whether the group of tables we reorder is built for DPsub with a conflict detector: semi/anti
        /// and `FULL` joins join it, and outer joins come in with both of their sides. Decided once per
        /// query in `optimizeJoinLogicalImpl`, which also counts the tables: only DPsub can plan such a
        /// group, and it turns down more tables than `DPSUB_MAX_RELATIONS`.
        bool allow_semi_anti_flattening = false;

        BuilderContext(
            const QueryPlanOptimizationSettings & optimization_settings_,
            const QueryPlan::Node & root_node,
            const JoinSettings & join_settings_,
            const SortingStep::Settings & sorting_settings_)
            : optimization_settings(optimization_settings_)
            , statistics_context(optimization_settings_, root_node)
            , join_settings(join_settings_)
            , sorting_settings(sorting_settings_)
            , effective_randomize_seed(optimization_settings_.query_plan_optimize_join_order_randomize)
        {
        }
    };

    std::shared_ptr<BuilderContext> context;

    /// Whether this (sub)graph lies inside a null-supplying input of an outer, semi or anti join
    /// above it, see `canFlattenThroughExpression`.
    bool below_null_supplying = false;

    explicit QueryGraphBuilder(std::shared_ptr<BuilderContext> context_)
        : context(std::move(context_)) {}

    QueryGraphBuilder(const QueryPlanOptimizationSettings & optimization_settings_, const QueryPlan::Node & root_node,
                      const JoinSettings & join_settings_, const SortingStep::Settings & sorting_settings_)
        : context(std::make_shared<BuilderContext>(optimization_settings_, root_node, join_settings_, sorting_settings_))
    {}

    bool hasCompatibleSettings(const JoinStepLogical & join_step) const
    {
        return context && join_step.getJoinSettings() == context->join_settings; // && join_step.getSortingSettings() == context->sorting_settings;
    }
};

static void uniteGraphs(QueryGraphBuilder & lhs, QueryGraphBuilder rhs)
{
    size_t shift = lhs.relation_stats.size();

    auto rhs_edges_raw = std::ranges::to<std::vector>(rhs.join_edges | std::views::transform([](const auto & e) { return e.getNode(); }));
    auto rhs_outer_conditions_raw = std::ranges::to<std::unordered_map>(rhs.outer_join_conditions | std::views::transform([](const auto & e) { return std::make_pair(e.first.getNode(), e.second); }));

    auto [rhs_actions_dag, rhs_expression_sources] = rhs.expression_actions.detachActionsDAG();

    lhs.expression_actions.getActionsDAG()->unite(std::move(rhs_actions_dag));
    for (auto & [node, sources] : rhs_expression_sources)
        sources.shift(shift);
    lhs.expression_actions.setNodeSources(rhs_expression_sources);

    lhs.relation_stats.append_range(std::move(rhs.relation_stats));
    lhs.inputs.append_range(std::move(rhs.inputs));

    lhs.join_edges.append_range(rhs_edges_raw | std::views::transform([&](auto p) { return JoinActionRef(p, lhs.expression_actions); }));
    /// The operators of `rhs` are appended after those of `lhs` below.
    const size_t op_shift = lhs.conflict_ops.size();
    lhs.join_edge_ops.append_range(rhs.join_edge_ops | std::views::transform([&](size_t op) { return op + op_shift; }));

    for (auto & [id, restriction] : rhs.join_kinds)
    {
        restriction.first.shift(shift);
        lhs.join_kinds[id + shift] = std::move(restriction);
    }

    /// Shift each captured CD-A operator's relation sets into the parent's numbering and append.
    for (auto & op : rhs.conflict_ops)
    {
        op.left.shift(shift);
        op.right.shift(shift);
        op.nel.shift(shift);
        op.nr_rels.shift(shift);
        lhs.conflict_ops.push_back(std::move(op));
    }

    for (auto & [sources, nodes] : rhs.type_changes)
        lhs.type_changes[sources + shift] = std::move(nodes);

    for (auto & [action, null_side] : rhs_outer_conditions_raw)
    {
        null_side.shift(shift);
        lhs.outer_join_conditions[JoinActionRef(action, lhs.expression_actions)] = std::move(null_side);
    }

    rhs.null_supplying_subtree_relations.shift(shift);
    lhs.null_supplying_subtree_relations |= rhs.null_supplying_subtree_relations;
    lhs.requires_conflict_detector = lhs.requires_conflict_detector || rhs.requires_conflict_detector;

    for (auto & [left, right] : rhs.cross_product_links)
    {
        left.shift(shift);
        right.shift(shift);
        lhs.cross_product_links.emplace_back(std::move(left), std::move(right));
    }

    /// The mark belongs to the flattened graph as a whole: reordering rebuilds every join step from it,
    /// so a mark carried by any join of the merged sub-graph has to survive the merge.
    lhs.disjunctions_optimization_applied |= rhs.disjunctions_optimization_applied;
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit);

void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
static void estimateJoinInPlace(JoinStepLogical & join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

/// Gives the join an estimate: by reordering it, or in place when the planner forbade reordering
/// (a decorrelated subquery, a forced order).
static void ensureJoinEstimated(JoinStepLogical & join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!join_step.isOptimized())
        optimizeJoinLogicalImpl(&join_step, node, nodes, optimization_settings);
    else if (!join_step.hasRowsEstimation())
        estimateJoinInPlace(join_step, node, nodes, optimization_settings);
}

/// Estimates the join a single-child chain below `node` ends in, so that the estimate of the
/// subquery the chain forms is known. A chain that reads a shared subplan, directly or through a
/// buffer, continues in that subplan.
static void optimizeJoinsBelow(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * child_node = node;
    while (true)
    {
        if (child_node->children.size() == 1)
            child_node = child_node->children[0];
        else if (const auto * reference = typeid_cast<const CommonSubplanReferenceStep *>(child_node->step.get()))
            child_node = reference->getSubplanReferenceRoot();
        else if (const auto * buffer_read = typeid_cast<const ReadFromCommonBufferStep *>(child_node->step.get()))
            child_node = buffer_read->getSubplanRoot();
        else
            break;
    }

    if (auto * child_join_step = typeid_cast<JoinStepLogical *>(child_node->step.get()))
        ensureJoinEstimated(*child_join_step, *child_node, nodes, optimization_settings);
}

/// A join the join order optimizer leaves in place (a residual condition, a locality, a `PASTE`
/// join, or join ordering turned off) still takes an estimate from its inputs with the formulas of
/// the join order optimizer; without it everything above the join is unknown. The residual
/// condition is not priced.
static void estimateJoinInPlace(JoinStepLogical & join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    for (auto * child : node.children)
        optimizeJoinsBelow(child, nodes, optimization_settings);

    const RelationStats left_stats = estimateReadRowsCount(*node.children[0]);
    const RelationStats right_stats = estimateReadRowsCount(*node.children[1]);
    const auto join = estimateJoin(left_stats, right_stats, join_step.getJoinOperator(), join_step.probeKeysWithinPreservedSide());

    auto input_estimate = [](const RelationStats & stats)
    {
        RelationEstimateInfo estimate;
        estimate.estimated_rows = stats.estimated_rows;
        estimate.max_rows = stats.max_rows;
        estimate.source = stats.source;
        estimate.imprecise_estimate = stats.imprecise_estimate;
        estimate.estimate_from_defaults = stats.estimate_from_defaults;
        return estimate;
    };
    join_step.setInPlaceEstimation(
        join.rows,
        join.column_stats,
        left_stats.imprecise_estimate || right_stats.imprecise_estimate,
        join.selectivity,
        join.max_rows,
        join.estimate_from_defaults,
        input_estimate(left_stats),
        input_estimate(right_stats));

    /// Annotate the plan nodes like the join-order path does, so that `EXPLAIN estimates` and the
    /// profile log see the estimate. A node that already carries an annotation keeps it: a plan
    /// fragment arrives with the estimates of the planner that decided it.
    if (!node.children[0]->cost_estimation)
        node.children[0]->cost_estimation = toCostEstimationInfo(left_stats);
    if (!node.children[1]->cost_estimation)
        node.children[1]->cost_estimation = toCostEstimationInfo(right_stats);
    if (!node.cost_estimation)
        node.cost_estimation = CostEstimationInfo{
            .rows = join.rows ? std::optional<Float64>(Float64(*join.rows)) : std::nullopt,
            .cost = std::nullopt,
            .source = RowEstimateSource::NoSource,
            .max_rows = join.max_rows ? std::optional<Float64>(Float64(*join.max_rows)) : std::nullopt,
            .estimate_from_defaults = join.estimate_from_defaults};
}

static String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format("{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? toString(stats.estimated_rows.value()) : "unknown",
        fmt::join(stats.column_stats | std::views::transform(
            [](const auto & p)
            {
                return fmt::format("{}: {}", p.first, p.second.num_distinct_values);
            }), ", "));
}


void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);

constexpr bool isInnerOrCross(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

/// Whether DPsub is the algorithm that will plan this query. Algorithms are tried in order, so when
/// DPsub comes first it plans everything it accepts and the rest only see what it turns down.
static bool dpsubLeadsJoinOrderChain(const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto & algorithms = optimization_settings.query_plan_optimize_join_order_algorithm;
    return !algorithms.empty() && algorithms.front() == JoinOrderAlgorithm::DPSUB;
}

/// Semi/anti joins can be moved around freely, rather than just swapped, only with a conflict
/// detector on and DPsub planning. Those joins do not commute, and DPsub with a detector is the
/// only combination that knows it -- any other algorithm would reorder them into a wrong plan.
/// A later algorithm in the chain is allowed: a graph that flattened them is marked and only
/// DPsub will solve it, and the relation count is checked before flattening so DPsub accepts it.
static bool conflictDetectorReordersSemiAnti(const QueryPlanOptimizationSettings & optimization_settings)
{
    return optimization_settings.query_plan_optimize_join_order_conflict_detector != JoinOrderConflictDetector::NONE
        && dpsubLeadsJoinOrderChain(optimization_settings);
}

/// `mergeInplace` binds a merged expression's inputs to the graph's outputs by `result_name`, and it
/// installs only the expression's outputs. An output named like one of its own inputs therefore
/// leaves a computed node over an input of that name, and resolving the name again applies the
/// expression a second time, so such an expression must not be merged into a join graph.
static bool hasOutputShadowingInputName(const ActionsDAG & dag)
{
    std::unordered_set<std::string_view> input_names;
    for (const auto * input : dag.getInputs())
        input_names.insert(input->result_name);

    for (const auto * output : dag.getOutputs())
    {
        if (output->type != ActionsDAG::ActionType::INPUT && input_names.contains(output->result_name))
            return true;
    }

    return false;
}

/// An `ExpressionStep` above a join may be merged into the flattened join graph when the setting
/// allows it and the expression cannot be applied twice by the name-based merge.
///
/// Merging puts the expression into the join graph, where reordering can leave it computed twice from the
/// raw inputs - once for the join key that decides matching and once for the output column - and can also
/// evaluate it on rows the original join order would have discarded. An expression whose result or whose
/// side effects depend on how many times and on which rows it runs is therefore not safe to merge: a
/// non-deterministic function draws independently in the two places, so the returned rows can violate the
/// query's own `JOIN ON` condition, a stateful function (`aiEmbed`, `timeSeriesStoreTags`, ...) makes
/// extra external calls or mutates per-query state, and a function with observable side effects (`sleep`)
/// spends a different amount of time and accounts different profile events.
static bool canMergeExpressionIntoJoinGraph(const ActionsDAG & dag, bool merge_expression_into_join)
{
    if (!merge_expression_into_join)
        return false;

    if (isSensitiveToEvaluationCount(dag))
        return false;

    return !hasOutputShadowingInputName(dag);
}

/// Whether a child join step may be flattened into the group of tables to reorder. Without a
/// conflict detector only plain inner and one-sided outer joins can. For the conflict detector,
/// which describes each operator by its two whole subtrees, semi/anti and `FULL` joins can too.
static bool isFlattenableChildJoin(JoinKind kind, JoinStrictness strictness, bool for_conflict_detector)
{
    const bool kind_allowed = isInnerOrCross(kind) || isLeft(kind) || isRight(kind) || (for_conflict_detector && isFull(kind));
    const bool strictness_allowed = strictness == JoinStrictness::All
        || (for_conflict_detector && (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti));
    return kind_allowed && strictness_allowed;
}

/// Whether the given input of a join step may be flattened into the group of tables to reorder.
/// Without a conflict detector only the preserved side of an outer join can: `join_kinds` describes
/// a null-supplying side of a single relation only. For the conflict detector either side of an
/// outer (or semi/anti) join can.
static bool isFlattenableJoinSide(JoinKind kind, JoinTableSide side, bool for_conflict_detector)
{
    if (isInnerOrCross(kind))
        return true;
    if (for_conflict_detector && (isLeft(kind) || isRight(kind) || isFull(kind)))
        return true;
    return side == JoinTableSide::Left ? isLeft(kind) : isRight(kind);
}

/// Whether the given input of a join step is null-supplying: padded with defaults or `NULL` for the
/// rows of the other input without a match, or, for a semi/anti join, read only through its matches.
static bool isNullSupplyingSide(JoinKind kind, JoinTableSide side)
{
    return side == JoinTableSide::Left ? isRightOrFull(kind) : isLeftOrFull(kind);
}

/// Whether every output of the expression is one of its inputs, possibly renamed: the projection of a
/// subquery that only selects columns.
static bool onlyRenamesColumns(const ActionsDAG & dag)
{
    for (const auto * output : dag.getOutputs())
    {
        const auto * node = output;
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();
        if (node->type != ActionsDAG::ActionType::INPUT)
            return false;
    }
    return true;
}

/// Whether a child join under an `ExpressionStep` may be flattened, looking through the expression.
/// A passthrough expression can always be looked through. Any other one is merged into the join graph
/// and evaluated after the reordered joins. Inside a null-supplying input, that is after the outer
/// join has padded the columns it reads, while the query computes it before: the padded row must get
/// the default of the computed column, not the expression applied to padded inputs (`bx + 1` gives 1,
/// not 0). So there only an expression that renames columns, which padding does not change, is merged;
/// any other keeps the child join a single relation.
static bool canFlattenThroughExpression(const ActionsDAG & dag, bool merge_expression_into_join, bool below_null_supplying)
{
    if (dag.hasArrayJoin())
        return false;
    if (isPassthroughActions(dag))
        return true;
    if (below_null_supplying && !onlyRenamesColumns(dag))
        return false;
    return canMergeExpressionIntoJoinGraph(dag, merge_expression_into_join);
}

static size_t addChildQueryGraph(
    QueryGraphBuilder & graph, QueryPlan::Node * node, QueryPlan::Nodes & nodes, const String & label, int join_steps_limit,
    bool below_null_supplying)
{
    auto * join_node = node;
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (expression_step && node->children.size() == 1  && !expression_step->getExpression().hasArrayJoin())
    {
        if (isPassthroughActions(expression_step->getExpression()))
        {
            /// Just skip trivial expression step, it doesn't change any columns
            expression_step = nullptr;
            join_node = node->children[0];
            node = node->children[0];
        }
        else if (canFlattenThroughExpression(
                     expression_step->getExpression(), graph.context->optimization_settings.merge_expression_into_join, below_null_supplying))
        {
            join_node = node->children[0];
        }
    }

    {
        auto * child_join_step = typeid_cast<JoinStepLogical *>(join_node->step.get());
        if (child_join_step && !child_join_step->isOptimized())
        {
            /// Normally only plain joins enter the group of tables we reorder. For the conflict
            /// detector semi/anti and `FULL` joins come along too, so DPsub can reorder them as well.
            bool allow_child_join_kind = isFlattenableChildJoin(
                child_join_step->getJoinOperator().kind, child_join_step->getJoinOperator().strictness,
                graph.context->allow_semi_anti_flattening);
            /// Do not flatten joins that have type-changing sides (e.g., LEFT JOIN
            /// with `join_use_nulls` making right-side columns Nullable). Flattening
            /// such joins allows the optimizer to reorder them, which can separate
            /// a relation from the join that causes its type change, leading to
            /// type changes being applied at the wrong step and the exception
            /// "Cannot fold actions for projection".
            allow_child_join_kind = allow_child_join_kind && child_join_step->typeChangingSides().empty();
            if (graph.hasCompatibleSettings(*child_join_step) && join_steps_limit > 1 && allow_child_join_kind)
            {
                QueryGraphBuilder child_graph(graph.context);
                child_graph.below_null_supplying = below_null_supplying;
                buildQueryGraph(child_graph, *join_node, nodes, join_steps_limit);

                if (expression_step)
                {
                    ActionsDAG::NodeMapping node_mapping;
                    child_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_step->getExpression()), node_mapping, true);
                }

                size_t count = child_graph.inputs.size();
                uniteGraphs(graph, std::move(child_graph));
                return count;
            }
            /// Optimize child subplan before continuing to get size estimation
            optimizeJoinLogicalImpl(child_join_step, *join_node, nodes, graph.context->optimization_settings);
        }
    }

    /// When the leaf is a subquery with Join-s wrapped in Expression/Aggregating steps, we cannot Joins to the graph, but we want to optimize
    /// those child Join to get proper statistics to use in the parent Join reordering.
    optimizeJoinsBelow(node, nodes, graph.context->optimization_settings);

    graph.inputs.push_back(node);
    RelationStats stats = estimateReadRowsCount(*node);

    std::optional<size_t> num_rows_from_cache = graph.context->statistics_context.getCachedHint(node);
    const bool use_rows_from_cache = graph.context->join_settings.use_hash_table_stats_for_join_reordering && num_rows_from_cache
        && (!stats.estimated_rows || num_rows_from_cache.value() < stats.estimated_rows.value());
    if (use_rows_from_cache)
    {
        /// The row count a past execution built the hash table from beats a larger statistics
        /// estimate, but it describes that execution's input, not this one's, so it stays imprecise,
        /// and a past count of zero does not prove the input empty.
        stats.estimated_rows = std::max<size_t>(1, *num_rows_from_cache);
        stats.imprecise_estimate = true;
        stats.source = RowEstimateSource::HashTableCache;
    }

    if (!label.empty())
        stats.table_name = label;

    if (UInt64 seed = graph.context->effective_randomize_seed)
        stats = getRandomizedStats(seed, graph.relation_stats.size(), stats.table_name, *node->step->getOutputHeader());

    LOG_TRACE(getLogger("optimizeJoin"), "Estimated statistics{} for {} {}",
        use_rows_from_cache ? " (from cache)" : (num_rows_from_cache ? " (cache not used)" : ""),
        node->step->getName(), dumpStatsForLogs(stats));
    graph.relation_stats.push_back(stats);
    return 1;
}

/// Names of scalar functions that propagate NULL: if any argument is NULL, the result is NULL.
/// Only these let us conclude a wrapped column reference is null when the relation's columns are.
/// The set is intentionally small and conservative -- an unknown function is treated as opaque
/// (contributes nothing), which can only make CD-A miss a valid reordering, never admit an invalid
/// one. It excludes NULL-blocking functions on purpose (`coalesce`, `ifNull`, `assumeNotNull`, ...).
static bool isNullPropagatingFunction(const ActionsDAG::Node & node)
{
    static const std::unordered_set<std::string_view> names = {
        /// comparisons (the atoms of equi/theta-join predicates)
        "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
        /// arithmetic that may wrap a column inside a comparison, e.g. `a.x + 1 = b.y`
        "plus", "minus", "multiply", "divide", "divideExact", "modulo", "negate",
        "CAST", "_CAST",
    };
    const auto & name = node.function_base->getName();
    if (!names.contains(name))
        return false;
    /// The operands of a join key comparison are brought to a common supertype only when the physical
    /// join is built (`predicateOperandsToCommonType`), after reordering. With a `Variant`/`Dynamic`/
    /// `JSON` operand that supertype is `Variant`/`Dynamic`, so a `Nullable` operand is cast to it and a
    /// NULL key then matches a NULL key: the comparison does not reject a null-extended row.
    if (std::ranges::any_of(node.children, [](const auto * child)
            { return isVariant(child->result_type) || isDynamic(child->result_type) || isObject(child->result_type); }))
        return false;
    /// A cast to a non-`Nullable` type raises `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN` rather than
    /// returning `NULL`; a cast to `Variant`/`Dynamic` returns `NULL` but a join on such a key
    /// matches `NULL` to `NULL`. Neither shape rejects a null-extended row, so neither counts.
    if (name == "CAST" || name == "_CAST")
        return isNullableOrLowCardinalityNullable(node.result_type);
    return true;
}

/// Whether an outer join of the join graph pads a column of this type with NULL. Only a join that changes
/// no column types is flattened into the graph (see `addChildQueryGraph`), so whatever `join_use_nulls`
/// it ran with, it pads a `Nullable` column with NULL and any other column with the type default: `0`,
/// `''`, `[]`, or an internal NULL of `Variant`/`Dynamic`, which another such key matches rather than
/// rejects. A subquery's join may also have run with a `join_use_nulls` other than the query's.
static bool nullExtensionIsNull(const DataTypePtr & type)
{
    return isNullableOrLowCardinalityNullable(type);
}

/// Relations R such that `node` evaluates to NULL when all of R's columns are NULL ("strict" on R).
/// Recurses only through null-propagating functions; any other node is opaque and contributes {}.
static BitSet strictOnRelations(const ActionsDAG::Node * node, const JoinExpressionActions & actions)
{
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
        case ActionsDAG::ActionType::PLACEHOLDER:
            if (!nullExtensionIsNull(node->result_type))
                return {};
            /// A leaf column reference is null exactly on its own relation.
            return JoinActionRef(node, actions).getSourceRelations();
        case ActionsDAG::ActionType::ALIAS:
            return node->children.empty() ? BitSet{} : strictOnRelations(node->children.front(), actions);
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node->function_base || !isNullPropagatingFunction(*node))
                return {};
            BitSet result;
            for (const auto * child : node->children)
                result |= strictOnRelations(child, actions);
            return result;
        }
        case ActionsDAG::ActionType::COLUMN:
        case ActionsDAG::ActionType::ARRAY_JOIN:
            return {};
    }
    return {};
}

/// Relations R such that the boolean `node` is false or unknown when all of R's columns are NULL
/// (null-rejecting). Conservative: when unsure it returns a subset of the true answer, which only
/// tightens the reordering constraints downstream and so stays correct.
static BitSet predicateNullRejectingRelations(const ActionsDAG::Node * node, const JoinExpressionActions & actions)
{
    if (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        return predicateNullRejectingRelations(node->children.front(), actions);

    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
    {
        const auto & name = node->function_base->getName();
        /// AND is rejecting on R if either conjunct is (a false/unknown conjunct makes AND false/unknown).
        if (name == "and")
        {
            BitSet result;
            for (const auto * child : node->children)
                result |= predicateNullRejectingRelations(child, actions);
            return result;
        }
        /// OR is rejecting on R only if both disjuncts are.
        if (name == "or" && !node->children.empty())
        {
            BitSet result = predicateNullRejectingRelations(node->children.front(), actions);
            for (size_t i = 1; i < node->children.size(); ++i)
                result = result & predicateNullRejectingRelations(node->children[i], actions);
            return result;
        }
    }

    /// Otherwise the predicate is rejecting wherever it becomes NULL (its false path is ignored,
    /// which is the safe under-approximation). Covers comparisons and bare nullable-bool columns.
    return strictOnRelations(node, actions);
}

void buildQueryGraph(QueryGraphBuilder & query_graph, QueryPlan::Node & node, QueryPlan::Nodes & nodes, int join_steps_limit)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical expected");
    query_graph.disjunctions_optimization_applied |= join_step->isDisjunctionsOptimizationApplied();
    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    QueryPlan::Node * lhs_plan = node.children[0];
    QueryPlan::Node * rhs_plan = node.children[1];
    auto [lhs_label, rhs_label] = join_step->getInputLabels();
    auto join_kind = join_step->getJoinOperator().kind;

    const bool for_conflict_detector = query_graph.context->allow_semi_anti_flattening;
    auto type_changing_sides = join_step->typeChangingSides();
    bool allow_left_subgraph = !type_changing_sides.contains(JoinTableSide::Left)
        && isFlattenableJoinSide(join_kind, JoinTableSide::Left, for_conflict_detector);
    bool allow_right_subgraph = !type_changing_sides.contains(JoinTableSide::Right)
        && isFlattenableJoinSide(join_kind, JoinTableSide::Right, for_conflict_detector);
    const bool lhs_below_null_supplying = query_graph.below_null_supplying || isNullSupplyingSide(join_kind, JoinTableSide::Left);
    const bool rhs_below_null_supplying = query_graph.below_null_supplying || isNullSupplyingSide(join_kind, JoinTableSide::Right);

    /// Check if flattening children would cause column name clashes between sides.
    if (allow_left_subgraph || allow_right_subgraph)
    {
        auto get_exposed_columns = [&](QueryPlan::Node * plan, bool allow_flatten, bool below_null_supplying) -> NameSet
        {
            NameSet names;
            auto * check = plan;
            if (allow_flatten)
            {
                bool merge_expression_into_join = query_graph.context->optimization_settings.merge_expression_into_join;
                auto * expr = typeid_cast<ExpressionStep *>(check->step.get());
                if (expr && canFlattenThroughExpression(expr->getExpression(), merge_expression_into_join, below_null_supplying))
                {
                    check = check->children[0];
                }
                auto * js = typeid_cast<JoinStepLogical *>(check->step.get());
                if (js && !js->isOptimized() && check->children.size() == 2)
                {
                    for (auto * child : check->children)
                        for (const auto & col : *child->step->getOutputHeader())
                            names.insert(col.name);
                    return names;
                }
            }
            for (const auto & col : *plan->step->getOutputHeader())
                names.insert(col.name);
            return names;
        };

        auto lhs_cols = get_exposed_columns(lhs_plan, allow_left_subgraph, lhs_below_null_supplying);
        auto rhs_cols = get_exposed_columns(rhs_plan, allow_right_subgraph, rhs_below_null_supplying);

        if (std::ranges::any_of(lhs_cols, [&](const auto & name) { return rhs_cols.contains(name); }))
        {
            LOG_DEBUG(getLogger("optimizeJoin"), "Column name clash detected between join sides, disabling subgraph flattening");
            allow_left_subgraph = false;
            allow_right_subgraph = false;
        }
    }

    size_t lhs_count = addChildQueryGraph(
        query_graph, lhs_plan, nodes, lhs_label, allow_left_subgraph ? join_steps_limit - 1 : 0, lhs_below_null_supplying);
    size_t rhs_count = addChildQueryGraph(
        query_graph, rhs_plan, nodes, rhs_label, allow_right_subgraph ? static_cast<int>(join_steps_limit - lhs_count) : 0, rhs_below_null_supplying);

    size_t total_inputs = query_graph.inputs.size();

    chassert(lhs_count && rhs_count && lhs_count + rhs_count == total_inputs && query_graph.relation_stats.size() == total_inputs);

    auto [expression_actions, join_operator] = join_step->detachExpressions();

    auto get_raw_nodes = std::views::transform([](const auto & ref) { return ref.getNode(); });
    auto join_expression = std::ranges::to<std::vector>(join_operator.expression | get_raw_nodes);
    auto residual_filter = std::ranges::to<std::vector>(join_operator.residual_filter | get_raw_nodes);

    auto [expression_actions_dag, expression_actions_sources] = expression_actions.detachActionsDAG();

    auto existing_outputs = std::ranges::to<std::unordered_set>(query_graph.expression_actions.getActionsDAG()->getOutputs());

    ActionsDAG::NodeMapping node_mapping;
    query_graph.expression_actions.getActionsDAG()->mergeInplace(std::move(expression_actions_dag), node_mapping, true);

    ActionsDAG::NodeRawConstPtrs join_outputs = query_graph.expression_actions.getActionsDAG()->getOutputs();

    JoinExpressionActions::NodeToSourceMapping new_sources;
    for (const auto & [old_node, sources] : expression_actions_sources)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        if (BitSet(sources).set(0, false).set(1, false))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression node {} is not a binary join: {}", old_node->result_name, toString(sources));
        if (lhs_count == 1 && sources.count() == 1 && sources.test(0))
            new_sources[new_node] = BitSet().set(0);
        if (rhs_count == 1 && sources.count() == 1 && sources.test(1))
            new_sources[new_node] = BitSet().set(total_inputs - 1);
    }
    query_graph.expression_actions.setNodeSources(new_sources);

    BitSet left_mask = BitSet::allSet(lhs_count);
    BitSet right_mask = BitSet::allSet(rhs_count);
    right_mask.shift(lhs_count);

    ActionsDAG::NodeRawConstPtrs left_changes_types;
    ActionsDAG::NodeRawConstPtrs right_changes_types;
    for (const auto * out_node : join_outputs)
    {
        if (out_node->type == ActionsDAG::ActionType::INPUT ||
            out_node->type == ActionsDAG::ActionType::COLUMN ||
            existing_outputs.contains(out_node))
            continue;

        auto source = JoinActionRef(out_node, query_graph.expression_actions).getSourceRelations();
        auto rel_id = source.getSingleBit();
        if (!rel_id.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine source relations for node {}", out_node->result_name);

        if (rel_id == 0)
            left_changes_types.push_back(out_node);
        else
            right_changes_types.push_back(out_node);
    }

    if (!left_changes_types.empty())
        query_graph.type_changes[0] = std::move(left_changes_types);
    if (!right_changes_types.empty())
        query_graph.type_changes[total_inputs - 1] = std::move(right_changes_types);

    BitSet join_expression_sources;
    /// Relations on which the whole ON clause rejects nulls, for the conflict detectors. The ON
    /// clause is the conjunction of the `join_expression` conjuncts, so a relation is rejecting for
    /// the operator as soon as any conjunct rejects on it -- hence the union.
    ///
    /// Null-rejection only counts on columns that an outer join pads with a real SQL NULL, the
    /// `Nullable` ones (see `nullExtensionIsNull`). Any other column is padded with a type default
    /// (`0`/`''`), which a "null-rejecting" predicate like `t2.id = t3.id` still matches; trusting it
    /// would unlock unsound assoc/asscom reorderings, e.g. `(t1 LEFT JOIN t2) LEFT JOIN t3` becoming
    /// `t1 LEFT JOIN (t2 LEFT JOIN t3)`, which changes the result.
    BitSet cda_nr_rels;
    for (const auto * old_node : join_expression)
    {
        const auto & new_node_entry = node_mapping.try_emplace(old_node, old_node);
        const auto * new_node = new_node_entry.first->second;
        auto & edge = query_graph.join_edges.emplace_back(new_node, query_graph.expression_actions);
        /// The operator of this join step, pushed to `conflict_ops` below.
        query_graph.join_edge_ops.push_back(query_graph.conflict_ops.size());

        /// Collect all sources from join expressions
        join_expression_sources |= edge.getSourceRelations();
        cda_nr_rels |= predicateNullRejectingRelations(new_node, query_graph.expression_actions);

        /// ON-clause predicates of an outer join must be applied exactly at the step
        /// that joins the null-supplying side, in its ON clause.
        /// Predicates of inner joins are filters: the optimizer may apply them at any
        /// step where all their source relations are available (as a post-join filter
        /// when that step is an outer join).
        if (isFull(join_kind))
            query_graph.outer_join_conditions[edge] = left_mask | right_mask;
        else if (isRight(join_kind))
            query_graph.outer_join_conditions[edge] = left_mask;
        else if (isLeft(join_kind))
            query_graph.outer_join_conditions[edge] = right_mask;
    }

    /// Capture this operator for the CD-A conflict detector before `join_expression_sources` is
    /// stripped of the null-supplying singleton below. `left_mask`/`right_mask` are this
    /// operator's two input subtrees in the current (sub)graph's local numbering; `nel` is the
    /// full set of relations the ON clause references. Records are shifted into global numbering
    /// by `uniteGraphs`.
    query_graph.conflict_ops.push_back(ConflictJoinOp{left_mask, right_mask, join_expression_sources, cda_nr_rels, join_kind, join_operator.strictness});

    /// A cross product, or an inner join whose condition does not reference both sides (`ON 1`,
    /// `ON t2.x = 5`, which is a cross product followed by a filter), links its two sides; see
    /// `QueryGraph::cross_product_links`.
    const bool condition_spans_sides = areIntersecting(join_expression_sources, left_mask) && areIntersecting(join_expression_sources, right_mask);
    if (isCrossOrComma(join_kind) || (isInner(join_kind) && !condition_spans_sides))
        query_graph.cross_product_links.emplace_back(left_mask, right_mask);

    /// `join_kinds` describes a null-supplying side of a single relation. Only the conflict detector
    /// flattens a larger one (see `isFlattenableJoinSide`), and it goes by `conflict_ops` instead; the
    /// relations of such a side are only kept out of column equivalences, and the graph is left to
    /// DPsub. So is a `FULL` join among more than two relations, which the other algorithms can only
    /// swap.
    auto add_null_supplying_side = [&](size_t count, size_t relation, const BitSet & side)
    {
        if (count == 1)
        {
            join_expression_sources.set(relation, false);
            query_graph.join_kinds[relation] = std::make_pair(join_expression_sources, join_kind);
            return;
        }
        if (!for_conflict_detector)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "JoinStepLogical with {} join must have exactly one input on its null-supplying side, but has {}",
                toString(join_kind), count);
        query_graph.null_supplying_subtree_relations |= side;
        query_graph.requires_conflict_detector = true;
    };
    if (isRightOrFull(join_kind))
        add_null_supplying_side(lhs_count, 0, left_mask);
    if (isLeftOrFull(join_kind))
        add_null_supplying_side(rhs_count, total_inputs - 1, right_mask);
    if (isFull(join_kind) && total_inputs > 2)
        query_graph.requires_conflict_detector = true;

    if (!residual_filter.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Residual filter is not supported in join reorder");
}

static std::vector<DPJoinEntry *> getJoinTreePostOrderSequence(const DPJoinEntryPtr & root)
{
    std::vector<DPJoinEntry *> result;
    result.reserve(root->relations.count() * 2);

    std::vector<DPJoinEntry *> stack;
    stack.push_back(root.get());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (!node->isLeaf())
        {
            if (!node->left || !node->right)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join node should have both left and right children");

            /// In post-order, we want LEFT -> RIGHT -> NODE
            /// Since we're using a stack (LIFO), we push RIGHT then LEFT
            stack.push_back(node->right.get());
            stack.push_back(node->left.get());
        }

        result.push_back(node);
    }

    /// Reverse the order to get post-order traversal
    std::reverse(result.begin(), result.end());

    return result;
}

/// Find single input node that is parent of the node
/// Only aliases and cast/toNullable functions are allowed in the path, otherwise an logical error is thrown
static const ActionsDAG::Node * trackInputColumn(const ActionsDAG::Node * node)
{
    while (!node->children.empty())
    {

        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto & function_name = node->function_base->getName();
            if (function_name != "toNullable" && function_name != "_CAST" && function_name != "CAST")
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} is a function '{}', expected toNullable or CAST",
                    node->result_name, function_name);
        }

        size_t non_const_children = 0;
        for (const auto * child_node : node->children)
        {
            if (child_node->type == ActionsDAG::ActionType::COLUMN)
                continue;
            node = child_node;
            non_const_children++;
        }

        if (non_const_children != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} has {} non const children, expected 1", node->result_name, non_const_children);
    }
    return node;
}

constexpr bool isSwapOnlyJoinKind(JoinKind kind)
{
    return kind == JoinKind::Full;
}

constexpr bool isSwapOnlyJoinStrictness(JoinStrictness strictness)
{
    return strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti;
}

namespace
{

/// What the plan of one join order of a graph is built from. `JoinGraphStep` keeps it, so that the plans of the other
/// orders are built when the Cascades optimizer imports the graph.
struct JoinOrderPlanBuilder : public JoinGraphStep::OrderBuilder
{
    JoinOrderPlanBuilder(JoinSettings join_settings_, SortingStep::Settings sorting_settings_)
        : join_settings(std::move(join_settings_))
        , sorting_settings(std::move(sorting_settings_))
    {
    }

    QueryGraphBuilder query_graph_builder{nullptr};
    JoinExpressionActions global_expression_actions;
    std::shared_ptr<ActionsDAG> global_actions_dag;
    /// Input in global dag -> it's position
    std::unordered_map<const ActionsDAG::Node *, size_t> input_node_map;
    /// input_position -> (relation no, input)
    std::vector<std::pair<size_t, const ActionsDAG::Node *>> initial_input_nodes;
    std::unordered_map<BitSet, RelationEstimateInfo> leaf_relation_infos;
    std::vector<UInt8> leaf_imprecise;
    std::vector<UInt8> leaf_without_estimate;
    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;
    JoinStrictness join_strictness = JoinStrictness::All;
    UInt64 cluster_id = 0;
    bool disjunctions_optimization_applied = false;
    /// The orders share the entries of their common subtrees; `built_joins` refers to the entries, which these keep.
    DPJoinEntryPtr chosen_order;
    std::vector<DPJoinEntryPtr> other_orders;

    /// The splits of every set that Cascades gets with `cascades_join_enumeration`, see `selectSetSplits`.
    std::shared_ptr<RecordedJoinSplits> recorded_splits;
    struct SetSplits
    {
        UInt64 relations = 0;
        /// Positions in `recorded_splits->splits`; the winner of the set first.
        std::vector<size_t> splits;
    };
    std::vector<SetSplits> set_splits;
    /// The splits that the `JoinEnumeration` rule adds during the search, see `buildRemainingSets`.
    std::vector<SetSplits> remaining_set_splits;

    /// Chooses the splits Cascades gets for `mode` and returns true, or returns false with `reason` when the graph
    /// gets its orders imported instead.
    bool selectSetSplits(QueryPlanOptimizationSettings::JoinEnumeration mode, size_t max_join_expressions, size_t orders, String & reason);

    /// Builds the plan of the join tree `optimized` over `input_nodes`, in `nodes`, and returns its root. The plan of
    /// a subtree that an order built before is not built again: the node of that plan is used.
    QueryPlan::Node * build(const DPJoinEntryPtr & optimized, const std::vector<QueryPlan::Node *> & input_nodes, QueryPlan::Nodes & nodes);

    size_t otherOrdersCount() const override { return other_orders.size(); }

    QueryPlan::Node * buildOtherOrder(size_t index, const std::vector<QueryPlan::Node *> & inputs, QueryPlan::Nodes & nodes) override
    {
        if (index >= other_orders.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join order {} is not available", index);
        return build(other_orders[index], inputs, nodes);
    }

    std::optional<JoinGraphStep::SetPlan> buildSetPlan(const std::vector<QueryPlan::Node *> & inputs) override;
    std::optional<JoinGraphStep::SetPlan> buildRemainingSets() override;
    bool hasRemainingSets() const override { return !remaining_set_splits.empty(); }

private:
    /// The plan of a join entry, and the columns of its relations after it, see `current_input_nodes` in `build`.
    struct BuiltJoin
    {
        QueryPlan::Node * node = nullptr;
        std::vector<std::pair<size_t, const ActionsDAG::Node *>> input_nodes;
        RelationEstimateInfo relation_info;
    };
    std::unordered_map<const DPJoinEntry *, BuiltJoin> built_joins;

    /// The joins of the tree whose plans an order built before can be used, and the entries below them, which are
    /// not built. A plan can be used when it keeps every column the tree uses above it: the joins above differ
    /// between orders, and with transitive predicates they can join on another column of the same relation.
    struct ReusableJoins
    {
        std::unordered_map<const DPJoinEntry *, const BuiltJoin *> reused;
        std::unordered_set<const DPJoinEntry *> skipped;
    };
    ReusableJoins findReusableJoins(
        const DPJoinEntryPtr & root,
        const std::vector<DPJoinEntry *> & sequence,
        const std::unordered_map<const ActionsDAG::Node *, size_t> & usage_level_map) const;

    /// The predicates of the join of `left` and `right`, see `buildSetPlan`.
    std::vector<JoinActionRef> splitPredicates(const RecordedJoinSplits::Split & split);
    /// The input columns a set of two or more relations outputs, see `buildSetPlan`.
    std::vector<const ActionsDAG::Node *> setColumns(UInt64 relations) const;
    /// The global inputs `node` is computed from.
    const std::vector<const ActionsDAG::Node *> & inputsOf(const ActionsDAG::Node * node);

    std::unordered_map<const ActionsDAG::Node *, std::vector<const ActionsDAG::Node *>> inputs_of;
    std::vector<std::vector<const ActionsDAG::Node *>> edge_inputs;
    std::vector<const ActionsDAG::Node *> output_inputs;
    /// Builds the joins of `sets`; the sets they are split into are built already.
    JoinGraphStep::SetPlan buildSets(const std::vector<SetSplits> & sets);

    /// The output header, the output columns and the estimates of every set built so far.
    UInt64 all_relations = 0;
    std::unordered_map<UInt64, SharedHeader> set_headers;
    std::unordered_map<UInt64, std::vector<const ActionsDAG::Node *>> set_columns_by_relations;
    std::unordered_map<UInt64, RelationEstimateInfo> set_relation_infos;

    /// Equalities made for an equivalence class, by the two members they compare.
    std::map<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>, JoinActionRef> class_equalities;
};

JoinOrderPlanBuilder::ReusableJoins JoinOrderPlanBuilder::findReusableJoins(
    const DPJoinEntryPtr & root,
    const std::vector<DPJoinEntry *> & sequence,
    const std::unordered_map<const ActionsDAG::Node *, size_t> & usage_level_map) const
{
    std::unordered_map<const DPJoinEntry *, size_t> position;
    for (size_t i = 0; i < sequence.size(); ++i)
        position[sequence[i]] = i;

    auto keeps_used_columns = [&](const DPJoinEntry & entry, const BuiltJoin & built)
    {
        for (size_t input_pos = 0; input_pos < initial_input_nodes.size(); ++input_pos)
        {
            const auto & [relation, input_node] = initial_input_nodes[input_pos];
            if (!entry.relations.test(relation) || built.input_nodes[input_pos].second)
                continue;
            const auto level = usage_level_map.find(input_node);
            if (level != usage_level_map.end() && level->second > position.at(&entry))
                return false;
        }
        return true;
    };

    ReusableJoins result;
    std::vector<const DPJoinEntry *> stack{root.get()};
    while (!stack.empty())
    {
        const auto * entry = stack.back();
        stack.pop_back();
        if (entry->isLeaf())
            continue;
        const auto built = built_joins.find(entry);
        if (entry != root.get() && built != built_joins.end() && keeps_used_columns(*entry, built->second))
        {
            result.reused.emplace(entry, &built->second);
            std::vector<const DPJoinEntry *> below{entry->left.get(), entry->right.get()};
            while (!below.empty())
            {
                const auto * skipped = below.back();
                below.pop_back();
                result.skipped.insert(skipped);
                if (!skipped->isLeaf())
                {
                    below.push_back(skipped->left.get());
                    below.push_back(skipped->right.get());
                }
            }
            continue;
        }
        stack.push_back(entry->left.get());
        stack.push_back(entry->right.get());
    }
    return result;
}

QueryPlan::Node * JoinOrderPlanBuilder::build(
    const DPJoinEntryPtr & optimized, const std::vector<QueryPlan::Node *> & input_nodes, QueryPlan::Nodes & nodes)
{
    const auto & optimization_settings = query_graph_builder.context->optimization_settings;
    auto current_input_nodes = initial_input_nodes;
    auto relation_infos = leaf_relation_infos;
    auto sequence = getJoinTreePostOrderSequence(optimized);

    if (sequence.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join tree is empty");

    /// Mapping from node to maximal step position where it is used
    /// It's used to drop unused expressions
    std::unordered_map<const ActionsDAG::Node *, size_t> usage_level_map;
    {
        std::deque<std::pair<const ActionsDAG::Node *, size_t>> stack;

        /// Join expressions used by i-th join step
        for (size_t i = 0; i < sequence.size(); ++i)
        {
            const auto & entry = sequence[i];
            for (const auto & action : entry->join_operator.expression)
                stack.emplace_back(action.getNode(), i);
            for (const auto & action : entry->join_operator.residual_filter)
                stack.emplace_back(action.getNode(), i);
        }
        /// Outputs at the end
        for (const auto & out : global_actions_dag->getOutputs())
            stack.emplace_back(out, sequence.size());

        while (!stack.empty())
        {
            auto [node, level] = stack.back();
            stack.pop_back();

            /// Stack is sorted with respect to level
            /// If we processed node it means that it and its children used by higher level
            auto [_, inserted] = usage_level_map.try_emplace(node, level);
            if (!inserted)
                continue;

            for (const auto * child : node->children)
                stack.push_back(std::make_pair(child, level));
        }
    }

    const auto [reused_joins, skipped_entries] = findReusableJoins(optimized, sequence, usage_level_map);

    std::stack<QueryPlan::Node *> node_stack;
    /// Decide once whether this graph's per-entry strictnesses are authoritative. `buildPhysicalPlan`
    /// leaves every DP entry `All` except a semi/anti join that a conflict detector (CD-A/CD-C)
    /// admitted while reordering, which carries its own strictness. If any entry carries a specific
    /// (non-`All`) strictness the graph is mixed -- inner joins around a reordered semi/anti join,
    /// which may be nested anywhere, even under an inner top join -- and each entry's own strictness
    /// must be kept. Otherwise the graph is uniform and its single `join_strictness` must be stamped
    /// onto the reconstructed joins (e.g. an ANY/SEMI/ANTI join that was only swapped, or the whole
    /// graph under the default greedy algorithm, which never populates per-entry strictness).
    bool graph_has_mixed_strictness = false;
    for (const auto * seq_entry : sequence)
        if (!seq_entry->isLeaf() && seq_entry->join_operator.strictness != JoinStrictness::All)
            graph_has_mixed_strictness = true;

    for (size_t entry_idx = 0; entry_idx < sequence.size(); ++entry_idx)
    {
        auto * entry = sequence[entry_idx];
        if (skipped_entries.contains(entry))
            continue;
        if (entry->isLeaf())
        {
            /// Base relation, use this step as input node
            size_t relation_id = safe_cast<size_t>(entry->relation_id);
            if (relation_id >= input_nodes.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid relation id: {}, input nodes size: {}", relation_id, input_nodes.size());
            /// `relation_infos` keeps the leaf estimates; the query graph itself was handed to the optimizer above.
            const auto & leaf_info = relation_infos.at(BitSet().set(relation_id));
            input_nodes[relation_id]->cost_estimation = CostEstimationInfo{
                .rows = leaf_info.estimated_rows ? std::optional<Float64>(Float64(*leaf_info.estimated_rows)) : std::nullopt,
                .cost = std::nullopt,
                .source = leaf_info.source,
                .max_rows = leaf_info.max_rows ? std::optional<Float64>(Float64(*leaf_info.max_rows)) : std::nullopt};
            node_stack.push(input_nodes[relation_id]);
        }
        else if (const auto built = reused_joins.find(entry); built != reused_joins.end())
        {
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
                if (entry->relations.test(current_input_nodes[input_pos].first))
                    current_input_nodes[input_pos].second = built->second->input_nodes[input_pos].second;
            relation_infos[entry->relations] = built->second->relation_info;
            node_stack.push(built->second->node);
        }
        else
        {
            /// Combine two nodes from the stack into a single join operation
            auto * left_child_node = node_stack.top();
            node_stack.pop();
            auto * right_child_node = node_stack.top();
            node_stack.pop();

            auto join_operator = entry->join_operator;
            /// See `graph_has_mixed_strictness` above: keep each entry's own strictness in a mixed
            /// graph, otherwise stamp the graph's single strictness.
            if (!graph_has_mixed_strictness)
                join_operator.strictness = join_strictness;

            /// The optimizer reconstructs an unconnected Inner pair (e.g. `INNER JOIN ... ON 1`,
            /// which produces no join edges) as Cross. That is equivalent only for ALL strictness:
            /// a Cross join ignores strictness, so `ANY INNER JOIN ... ON 1` would degrade to a full
            /// cartesian product. Restore Inner so that the physical join falls back to the
            /// constant-key join (`__lhs_const = __rhs_const`) that preserves strictness semantics.
            /// Goes by this join's own strictness: in a graph of mixed strictness (a reordered
            /// semi/anti join) a cross product among its plain joins is `All` and stays `Cross`.
            if (join_operator.strictness != JoinStrictness::All && join_operator.kind == JoinKind::Cross)
                join_operator.kind = JoinKind::Inner;
            auto left_rels = entry->left->relations;
            auto right_rels = entry->right->relations;

            bool has_prepared_storage_at_right = bool(typeid_cast<const JoinStepLogicalLookup *>(right_child_node->step.get()));
            bool has_prepared_storage_at_left = bool(typeid_cast<const JoinStepLogicalLookup *>(left_child_node->step.get()));

            auto lhs_estimation = entry->left->estimated_rows;
            auto rhs_estimation = entry->right->estimated_rows;

            /// The smaller side is built. A side without an estimate counts as smaller when its bound is below the
            /// other side's estimate; without this a join of unknown inputs would build the known large input.
            const auto lhs_known_smaller = [&]
            {
                if (lhs_estimation && rhs_estimation)
                    return lhs_estimation.value() < rhs_estimation.value();
                return !lhs_estimation && rhs_estimation && entry->left->max_rows && *entry->left->max_rows < *rhs_estimation;
            };
            bool swap_on_sizes = optimization_settings.join_swap_table.has_value()
                ? optimization_settings.join_swap_table.value()
                : entry->join_method == JoinMethod::Hash && lhs_known_smaller();

            bool flip_join = has_prepared_storage_at_left || (!has_prepared_storage_at_right && swap_on_sizes);

            /// fixme: USING clause handled specially in join algorithm, so swap breaks it
            /// At the time of writing, we're not able to swap inputs for ANY or SEMI partial merge join, because it only supports inner or left joins, but not right
            /// ANTI partial merge join is not supported for any join kind
            const bool partial_merge_join_can_be_selected = std::ranges::any_of(
                join_settings.join_algorithms,
                [](JoinAlgorithm alg)
                { return alg == JoinAlgorithm::PARTIAL_MERGE || alg == JoinAlgorithm::PREFER_PARTIAL_MERGE || alg == JoinAlgorithm::AUTO; });
            const bool should_worry_about_partial_merge_join = partial_merge_join_can_be_selected
                && (!MergeJoin::isSupported(join_operator.kind, join_operator.strictness)
                    || !MergeJoin::isSupported(reverseJoinKind(join_operator.kind), join_operator.strictness));
            bool skip_flip_any_join = join_settings.join_any_take_last_row && join_operator.strictness == JoinStrictness::Any;
            const bool suitable_swap_only_join = isSwapOnlyJoinStrictness(join_operator.strictness)
                && !should_worry_about_partial_merge_join
                && !skip_flip_any_join;
            if (join_operator.strictness != JoinStrictness::All && !suitable_swap_only_join)
                flip_join = false;

            if (flip_join)
            {
                /// For hash joins, we want to keep the smaller side on the right
                std::swap(left_rels, right_rels);
                std::swap(left_child_node, right_child_node);
                join_operator.kind = reverseJoinKind(join_operator.kind);
            }

            auto left_header_ptr = left_child_node->step->getOutputHeader();
            auto right_header_ptr = right_child_node->step->getOutputHeader();

            const auto & left_header = *left_header_ptr;
            const auto & right_header = *right_header_ptr;

            ActionsDAG::NodeRawConstPtrs required_output_nodes;

            /// input pos -> new input node
            std::unordered_map<size_t, const ActionsDAG::Node *> current_step_type_changes;

            for (auto rel_id : {left_rels.getSingleBit(), right_rels.getSingleBit()})
            {
                if (!rel_id.has_value())
                    continue;
                const auto & new_inputs = query_graph_builder.type_changes[rel_id.value()];
                for (const auto * new_input : new_inputs)
                {
                    const auto * input_node = trackInputColumn(new_input);
                    auto it = input_node_map.find(input_node);
                    if (it == input_node_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} not found in inputs of dag {}", input_node->result_name, global_actions_dag->dumpDAG());
                    current_step_type_changes[it->second] = new_input;
                }
            }

            auto joined_mask = entry->relations;
            ActionsDAG::NodeMapping current_inputs;
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                current_inputs[input_node] = input_node;

                const auto * out_node = input_node;
                if (auto it2 = current_step_type_changes.find(input_pos); it2 != current_step_type_changes.end())
                    out_node = it2->second;
                /// add input (possibly which changed type) to required output
                required_output_nodes.push_back(out_node);
            }

            for (const auto & action : join_operator.expression)
                required_output_nodes.push_back(action.getNode());
            for (const auto & action : join_operator.residual_filter)
                required_output_nodes.push_back(action.getNode());

            if (entry_idx == sequence.size() - 1)
            {
                for (const auto * output_node : global_actions_dag->getOutputs())
                    required_output_nodes.push_back(output_node);
            }

            JoinExpressionActions current_expression_actions(left_header, right_header,
                ActionsDAG::foldActionsByProjection(current_inputs, required_output_nodes));

            auto current_dag = current_expression_actions.getActionsDAG();
            auto & dag_outputs = current_dag->getOutputs();
            if (required_output_nodes.size() != dag_outputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output nodes size {} does not match current output nodes size in dag {}", required_output_nodes.size(), current_dag->dumpDAG());

            auto join_expression_map = std::ranges::to<ActionsDAG::NodeMapping>(std::views::zip(required_output_nodes, dag_outputs));
            auto remap_action = [&](JoinActionRef & action)
            {
                const auto * mapped_node = join_expression_map[action.getNode()];
                if (!mapped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} not found in current dag {}", action.getNode()->result_name, current_dag->dumpDAG());
                action = JoinActionRef(mapped_node, current_expression_actions);
            };
            for (auto & action : join_operator.expression)
                remap_action(action);
            for (auto & action : join_operator.residual_filter)
                remap_action(action);

            /// Setup outputs after join
            dag_outputs.clear();
            for (auto [input_pos, new_input] : current_step_type_changes)
            {
                current_input_nodes.at(input_pos).second = new_input;
            }

            const ActionsDAG::Node * first_dropped_node = nullptr;
            size_t first_dropped_node_pos = 0;
            /// Columns returned from JOIN is input with possibly corrected type
            for (size_t input_pos = 0; input_pos < current_input_nodes.size(); ++input_pos)
            {
                auto & [rel_idx, input_node] = current_input_nodes[input_pos];
                if (!joined_mask.test(rel_idx) || !input_node)
                    continue;

                if (usage_level_map[input_node] <= entry_idx)
                {
                    if (!first_dropped_node)
                    {
                        auto mapped_it = join_expression_map.find(input_node);
                        if (mapped_it == join_expression_map.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", input_node->result_name);
                        first_dropped_node = mapped_it->second;
                        first_dropped_node_pos = input_pos;
                    }
                    input_node = nullptr;
                    continue;
                }

                auto mapped_it = join_expression_map.find(input_node);
                if (mapped_it == join_expression_map.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", input_node->result_name);
                dag_outputs.push_back(mapped_it->second);
            }
            auto actions_after_join = dag_outputs;

            /// Last step, output should correspond to the global actions DAG
            if (entry_idx == sequence.size() - 1)
            {
                dag_outputs.clear();
                for (const auto * output_node : global_actions_dag->getOutputs())
                {
                    auto mapped_it = join_expression_map.find(output_node);
                    if (mapped_it == join_expression_map.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' not found in join expression map", output_node->result_name);
                    dag_outputs.push_back(mapped_it->second);
                    /// Include COLUMN Const nodes (like `__join_result_dummy`) in actions_after_join.
                    /// These are `fromNone` and will be placed in left_dag, ensuring the left pre-join
                    /// always produces at least one output column (so blocks have correct row count).
                    if (mapped_it->second->type == ActionsDAG::ActionType::COLUMN)
                        actions_after_join.push_back(mapped_it->second);
                }
            }

            if (actions_after_join.empty())
            {
                if (!first_dropped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned from join: {}", current_dag->dumpDAG());
                actions_after_join.push_back(first_dropped_node);
            }

            if (dag_outputs.empty())
            {
                if (!first_dropped_node)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns returned from join: {}", current_dag->dumpDAG());
                dag_outputs.push_back(first_dropped_node);
                current_input_nodes.at(first_dropped_node_pos).second = first_dropped_node;
            }

            auto join_step = std::make_unique<JoinStepLogical>(
                left_header_ptr,
                right_header_ptr,
                std::move(join_operator),
                std::move(current_expression_actions),
                actions_after_join,
                join_settings,
                sorting_settings);

            join_step->setDisjunctionsOptimizationApplied(disjunctions_optimization_applied);

            /// Diagnostic only: a join is imprecise if any of its leaves was, or had no estimate at
            /// all, in which case its cost stood on a bound or a placeholder.
            bool imprecise_estimate = false;
            bool costed_without_estimate = false;
            for (size_t i = 0; i < leaf_imprecise.size(); ++i)
            {
                if (!entry->relations.test(i))
                    continue;
                imprecise_estimate |= leaf_imprecise[i] || leaf_without_estimate[i];
                costed_without_estimate |= leaf_without_estimate[i];
            }
            if (costed_without_estimate)
                ProfileEvents::increment(ProfileEvents::JoinOrderJoinsCostedWithoutRowEstimate);

            join_step->setInputRelations(relation_infos[left_rels], relation_infos[right_rels]);
            relation_infos[entry->relations] = RelationEstimateInfo{
                .name = join_step->getReadableRelationName(),
                .estimated_rows = entry->estimated_rows,
                .max_rows = entry->max_rows,
                .imprecise_estimate = imprecise_estimate,
                .estimate_from_defaults = entry->estimate_from_defaults,
                .composite = true};

            join_step->setJoinOrderEstimation(
                entry->estimated_rows, entry->column_stats, imprecise_estimate, entry->cost, entry->selectivity, cluster_id, entry->max_rows,
                entry->estimate_from_defaults);

            auto & new_node = nodes.emplace_back();

            auto join_cache_keys = query_graph_builder.context->statistics_context
                .deriveCacheKeysForNewJoin(left_child_node, right_child_node, new_node, *join_step);
            if (join_cache_keys.right_key)
                join_step->setRightHashTableCacheKey(join_cache_keys.right_key);
            if (join_cache_keys.output_key)
                join_step->setJoinOutputCacheKey(join_cache_keys.output_key);

            new_node.step = std::move(join_step);
            new_node.children = {left_child_node, right_child_node};
            new_node.cost_estimation = CostEstimationInfo{
                .rows = entry->estimated_rows ? std::optional<Float64>(Float64(*entry->estimated_rows)) : std::nullopt,
                .cost = entry->cost,
                .source = RowEstimateSource::NoSource,
                .max_rows = entry->max_rows ? std::optional<Float64>(Float64(*entry->max_rows)) : std::nullopt,
                .estimate_from_defaults = entry->estimate_from_defaults};
            node_stack.push(&new_node);
            built_joins.emplace(entry, BuiltJoin{&new_node, current_input_nodes, relation_infos[entry->relations]});
        }
    }

    if (node_stack.size() != 1 || node_stack.top() != &nodes.back())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal join sequence produced: [{}]",
            fmt::join(sequence | std::views::transform([](const auto * e) { return e ? e->dump() : "null"; }), ", "));

    return node_stack.top();
}

const std::vector<const ActionsDAG::Node *> & JoinOrderPlanBuilder::inputsOf(const ActionsDAG::Node * node)
{
    if (auto it = inputs_of.find(node); it != inputs_of.end())
        return it->second;

    std::vector<const ActionsDAG::Node *> inputs;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack{node};
    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();
        if (!visited.insert(current).second)
            continue;
        if (current->type == ActionsDAG::ActionType::INPUT)
            inputs.push_back(current);
        for (const auto * child : current->children)
            stack.push_back(child);
    }
    return inputs_of.emplace(node, std::move(inputs)).first->second;
}

static UInt64 toRelationMask(const BitSet & relations)
{
    UInt64 mask = 0;
    for (auto relation : relations)
        mask |= UInt64(1) << relation;
    return mask;
}

/// The members of the equivalence classes, one list per class, each in the order of the global inputs.
static std::vector<std::vector<JoinActionRef>> sortedEquivalenceClasses(
    const EquivalenceClasses<JoinActionRef> & classes, const std::unordered_map<const ActionsDAG::Node *, size_t> & input_node_map)
{
    std::unordered_set<const void *> seen;
    std::vector<std::vector<JoinActionRef>> result;
    for (const auto & [member, members] : classes.getMemberToClassMap())
    {
        if (!seen.insert(members.get()).second)
            continue;
        std::vector<JoinActionRef> sorted(members->begin(), members->end());
        std::ranges::sort(sorted, {}, [&](const JoinActionRef & ref) { return input_node_map.at(ref.getNode()); });
        result.push_back(std::move(sorted));
    }
    std::ranges::sort(result, {}, [&](const auto & members) { return input_node_map.at(members.front().getNode()); });
    return result;
}

static std::optional<size_t> singleRelation(const JoinActionRef & ref)
{
    return ref.getSourceRelations().getSingleBit();
}

/// The splits of the `orders` cheapest complete join orders by the DPsub cost, by set. The cost of an order is the sum of
/// the local costs of its joins, so the cheapest plans of a set are among the combinations of the cheapest plans of the
/// two sides of each of its splits.
static std::unordered_map<UInt64, std::vector<size_t>> cheapestOrdersSplits(
    const RecordedJoinSplits & recorded_splits, const std::unordered_map<UInt64, std::vector<size_t>> & splits_by_set, size_t orders)
{
    struct Plan
    {
        double cost = 0;
        /// Position in `recorded_splits.splits`, none for a single relation.
        std::optional<size_t> split;
        size_t left_rank = 0;
        size_t right_rank = 0;
    };
    std::unordered_map<UInt64, std::vector<Plan>> cheapest;

    std::vector<UInt64> sets;
    for (const auto & [relations, _] : splits_by_set)
        sets.push_back(relations);
    std::ranges::sort(sets, {}, [](UInt64 relations) { return std::pair{std::popcount(relations), relations}; });

    auto plans_of = [&](UInt64 relations) -> const std::vector<Plan> &
    {
        if (std::popcount(relations) == 1)
            return cheapest.try_emplace(relations, std::vector<Plan>{Plan{}}).first->second;
        return cheapest.at(relations);
    };

    for (UInt64 relations : sets)
    {
        std::vector<Plan> candidates;
        for (size_t index : splits_by_set.at(relations))
        {
            const auto & split = recorded_splits.splits[index];
            const auto & left_plans = plans_of(split.left);
            const auto & right_plans = plans_of(split.right);
            for (size_t i = 0; i < left_plans.size(); ++i)
                for (size_t j = 0; j < right_plans.size(); ++j)
                    candidates.push_back({split.local_cost + left_plans[i].cost + right_plans[j].cost, index, i, j});
        }
        const size_t kept = std::min(orders, candidates.size());
        std::ranges::partial_sort(candidates, candidates.begin() + kept, {}, &Plan::cost);
        candidates.resize(kept);
        cheapest[relations] = std::move(candidates);
    }

    std::unordered_map<UInt64, std::vector<size_t>> result;
    std::vector<std::pair<UInt64, size_t>> stack;
    for (size_t rank = 0; rank < cheapest.at(sets.back()).size(); ++rank)
        stack.emplace_back(sets.back(), rank);
    while (!stack.empty())
    {
        auto [relations, rank] = stack.back();
        stack.pop_back();
        const auto & plan = cheapest.at(relations)[rank];
        if (!plan.split)
            continue;
        auto & indexes = result[relations];
        if (std::ranges::find(indexes, *plan.split) == indexes.end())
            indexes.push_back(*plan.split);
        const auto & split = recorded_splits.splits[*plan.split];
        stack.emplace_back(split.left, plan.left_rank);
        stack.emplace_back(split.right, plan.right_rank);
    }
    return result;
}

bool JoinOrderPlanBuilder::selectSetSplits(
    QueryPlanOptimizationSettings::JoinEnumeration mode, size_t max_join_expressions, size_t orders, String & reason)
{
    using JoinEnumeration = QueryPlanOptimizationSettings::JoinEnumeration;

    if (!recorded_splits || !recorded_splits->complete)
    {
        reason = "DPsub did not plan the graph";
        return false;
    }

    /// The equalities of a class connect columns of different relations only; two columns of one relation in a class
    /// would never be compared to each other, see `splitPredicates`.
    for (const auto & members : sortedEquivalenceClasses(recorded_splits->column_equivalences, input_node_map))
    {
        std::unordered_set<size_t> relations;
        for (const auto & member : members)
        {
            auto relation = singleRelation(member);
            if (!relation || !relations.insert(*relation).second)
            {
                reason = "an equivalence class has two columns of one relation";
                return false;
            }
        }
    }

    const auto & splits = recorded_splits->splits;
    if ((mode == JoinEnumeration::AllSplits || mode == JoinEnumeration::AllSplitsRule) && 2 * splits.size() > max_join_expressions)
    {
        reason = fmt::format("{} splits are more than `cascades_join_enumeration_max_splits` allows", splits.size());
        return false;
    }

    std::unordered_map<UInt64, std::vector<size_t>> splits_by_set;
    for (size_t i = 0; i < splits.size(); ++i)
        splits_by_set[splits[i].left | splits[i].right].push_back(i);

    auto find_split = [&](UInt64 relations, UInt64 left) -> std::optional<size_t>
    {
        auto it = splits_by_set.find(relations);
        if (it == splits_by_set.end())
            return std::nullopt;
        for (size_t index : it->second)
            if (splits[index].left == left || splits[index].right == left)
                return index;
        return std::nullopt;
    };

    std::unordered_map<UInt64, std::vector<size_t>> selected;
    if (mode == JoinEnumeration::AllSplits)
    {
        selected = splits_by_set;
    }
    else if (mode == JoinEnumeration::TopK)
    {
        selected = cheapestOrdersSplits(*recorded_splits, splits_by_set, orders);

        /// Every set also gets the split of its winner, which its statistics come from, see `order_sets` below. That
        /// split can lead to sets that no cheapest order has, so they get their winners too.
        std::vector<UInt64> unchecked;
        for (const auto & [relations, _] : selected)
            unchecked.push_back(relations);
        while (!unchecked.empty())
        {
            const UInt64 relations = unchecked.back();
            unchecked.pop_back();
            const auto & winner = recorded_splits->winners.at(relations);
            auto winner_index = find_split(relations, toRelationMask(winner->left->relations));
            if (!winner_index)
            {
                reason = "the winner of a set is not a recorded split";
                return false;
            }
            auto & indexes = selected[relations];
            if (std::ranges::find(indexes, *winner_index) != indexes.end())
                continue;
            indexes.push_back(*winner_index);
            for (UInt64 side : {splits[*winner_index].left, splits[*winner_index].right})
                if (std::popcount(side) > 1 && !selected.contains(side))
                {
                    selected[side];
                    unchecked.push_back(side);
                }
        }
    }
    else
    {
        /// The splits of the chosen order and of the other orders.
        std::vector<const DPJoinEntry *> stack{chosen_order.get()};
        for (const auto & order : other_orders)
            stack.push_back(order.get());
        std::unordered_set<const DPJoinEntry *> visited;
        while (!stack.empty())
        {
            const auto * entry = stack.back();
            stack.pop_back();
            if (entry->isLeaf() || !visited.insert(entry).second)
                continue;
            const UInt64 relations = toRelationMask(entry->relations);
            auto index = find_split(relations, toRelationMask(entry->left->relations));
            if (!index)
            {
                reason = "a join of the orders is not a recorded split";
                return false;
            }
            auto & set = selected[relations];
            if (std::ranges::find(set, *index) == set.end())
                set.push_back(*index);
            stack.push_back(entry->left.get());
            stack.push_back(entry->right.get());
        }
    }

    /// With the rule, the splits of the orders are imported, and the rule adds the others during the search.
    std::unordered_map<UInt64, std::vector<size_t>> remaining;
    if (mode == JoinEnumeration::AllSplitsRule)
    {
        for (const auto & [relations, indexes] : splits_by_set)
        {
            const auto it = selected.find(relations);
            for (size_t index : indexes)
                if (it == selected.end() || std::ranges::find(it->second, index) == it->second.end())
                    remaining[relations].push_back(index);
        }
    }

    /// The winner of a set first: Cascades derives the statistics of a group from its first expression. A set that is
    /// imported already has its winner there.
    auto order_sets = [&](std::unordered_map<UInt64, std::vector<size_t>> & sets, bool imported_already, std::vector<SetSplits> & result)
    {
        result.clear();
        for (auto & [relations, indexes] : sets)
        {
            if (!imported_already || !selected.contains(relations))
            {
                const auto & winner = recorded_splits->winners.at(relations);
                auto winner_index = find_split(relations, toRelationMask(winner->left->relations));
                if (!winner_index)
                {
                    reason = "the winner of a set is not a recorded split";
                    return false;
                }
                std::erase(indexes, *winner_index);
                indexes.insert(indexes.begin(), *winner_index);
            }
            result.push_back({relations, std::move(indexes)});
        }
        std::ranges::sort(result, {}, [](const SetSplits & set) { return std::pair{std::popcount(set.relations), set.relations}; });
        return true;
    };
    if (!order_sets(remaining, /* imported_already= */ true, remaining_set_splits))
        return false;
    if (!order_sets(selected, /* imported_already= */ false, set_splits))
        return false;

    edge_inputs.clear();
    for (const auto & edge : recorded_splits->edges)
        edge_inputs.push_back(edge ? inputsOf(edge.getNode()) : std::vector<const ActionsDAG::Node *>{});
    output_inputs.clear();
    std::unordered_set<const ActionsDAG::Node *> seen_outputs;
    for (const auto * output : global_actions_dag->getOutputs())
        for (const auto * input : inputsOf(output))
            if (seen_outputs.insert(input).second)
                output_inputs.push_back(input);
    std::ranges::sort(output_inputs, {}, [&](const auto * input) { return input_node_map.at(input); });
    return true;
}

std::vector<JoinActionRef> JoinOrderPlanBuilder::splitPredicates(const RecordedJoinSplits::Split & split)
{
    const auto & classes = recorded_splits->column_equivalences;
    std::vector<JoinActionRef> predicates;

    /// An equality between columns of two relations is replaced by the equalities of its class below: the members of
    /// a class are made equal by one equality at every join whose two sides both have members, so every join order of
    /// a set compares the same columns. An equality within one relation is a filter on its values and stays.
    for (size_t index : split.edges)
    {
        const auto & edge = recorded_splits->edges[index];
        auto [op, lhs, rhs] = edge.asBinaryPredicate();
        if (op == JoinConditionOperator::Equals)
        {
            auto lhs_input = lhs.resolveAliases();
            auto rhs_input = rhs.resolveAliases();
            auto lhs_relation = singleRelation(lhs_input);
            auto rhs_relation = singleRelation(rhs_input);
            if (lhs_input.getNode()->type == ActionsDAG::ActionType::INPUT && rhs_input.getNode()->type == ActionsDAG::ActionType::INPUT
                && lhs_relation && rhs_relation && *lhs_relation != *rhs_relation)
            {
                auto lhs_class = classes.getClass(lhs_input);
                if (lhs_class && lhs_class == classes.getClass(rhs_input))
                    continue;
            }
        }
        predicates.push_back(edge);
    }

    auto side_member = [](const std::vector<JoinActionRef> & members, UInt64 relations) -> const JoinActionRef *
    {
        for (const auto & member : members)
            if ((relations >> *singleRelation(member)) & 1)
                return &member;
        return nullptr;
    };
    for (const auto & members : sortedEquivalenceClasses(classes, input_node_map))
    {
        const auto * left_member = side_member(members, split.left);
        const auto * right_member = side_member(members, split.right);
        if (!left_member || !right_member)
            continue;
        auto key = std::pair{left_member->getNode(), right_member->getNode()};
        if (input_node_map.at(key.second) < input_node_map.at(key.first))
            std::swap(key.first, key.second);
        auto it = class_equalities.find(key);
        if (it == class_equalities.end())
            it = class_equalities.emplace(key, JoinActionRef::transform(
                {*left_member, *right_member}, JoinActionRef::AddFunction(JoinConditionOperator::Equals))).first;
        predicates.push_back(it->second);
    }
    return predicates;
}

std::vector<const ActionsDAG::Node *> JoinOrderPlanBuilder::setColumns(UInt64 relations) const
{
    std::set<size_t> positions;
    auto add = [&](const ActionsDAG::Node * input)
    {
        const size_t position = input_node_map.at(input);
        if ((relations >> initial_input_nodes[position].first) & 1)
            positions.insert(position);
    };

    /// The inputs of the predicates that apply above the set and of the graph's outputs.
    for (size_t i = 0; i < edge_inputs.size(); ++i)
        if (recorded_splits->edge_relations[i] & ~relations)
            for (const auto * input : edge_inputs[i])
                add(input);
    for (const auto * input : output_inputs)
        add(input);

    /// The members of a class that a join above the set compares, see `splitPredicates`.
    for (const auto & members : sortedEquivalenceClasses(recorded_splits->column_equivalences, input_node_map))
    {
        const bool outside = std::ranges::any_of(members, [&](const JoinActionRef & member) { return !((relations >> *singleRelation(member)) & 1); });
        if (outside)
            for (const auto & member : members)
                add(member.getNode());
    }

    /// One input of the lowest relation is always kept, so that a set has a column to carry its rows. The input with
    /// the lowest position of that relation is kept by every set that has the relation, so a join always finds it in
    /// one of its inputs.
    const size_t lowest_relation = std::countr_zero(relations);
    for (size_t position = 0; position < initial_input_nodes.size(); ++position)
    {
        if (initial_input_nodes[position].first == lowest_relation)
        {
            positions.insert(position);
            break;
        }
    }

    std::vector<const ActionsDAG::Node *> columns;
    for (size_t position : positions)
        columns.push_back(initial_input_nodes[position].second);
    return columns;
}

/// All joins of a set have the same predicates and output columns, whatever the split:
/// - the predicates are the recorded edges of the split, with the equalities between relations replaced by the
///   equalities of their classes, see `splitPredicates`;
/// - the outputs are the inputs that a join above the set needs, see `setColumns`; the set of all relations outputs
///   what the graph outputs.
/// Every join of a set also gets the estimates of the set's DPsub winner.
std::optional<JoinGraphStep::SetPlan> JoinOrderPlanBuilder::buildSetPlan(const std::vector<QueryPlan::Node *> & inputs)
{
    if (set_splits.empty())
        return std::nullopt;

    all_relations = (UInt64(1) << inputs.size()) - 1;
    for (size_t relation = 0; relation < inputs.size(); ++relation)
    {
        const UInt64 mask = UInt64(1) << relation;
        set_headers[mask] = inputs[relation]->step->getOutputHeader();
        for (const auto & [input_relation, input] : initial_input_nodes)
            if (input_relation == relation)
                set_columns_by_relations[mask].push_back(input);
        set_relation_infos[mask] = leaf_relation_infos.at(BitSet().set(relation));
    }
    ProfileEvents::increment(ProfileEvents::JoinEnumerationSetPlans);
    return buildSets(set_splits);
}

std::optional<JoinGraphStep::SetPlan> JoinOrderPlanBuilder::buildRemainingSets()
{
    if (remaining_set_splits.empty())
        return std::nullopt;
    if (set_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The remaining sets of a join graph are built before its sets");
    auto plan = buildSets(remaining_set_splits);
    remaining_set_splits.clear();
    return plan;
}

JoinGraphStep::SetPlan JoinOrderPlanBuilder::buildSets(const std::vector<SetSplits> & sets)
{
    auto & headers = set_headers;
    auto & columns = set_columns_by_relations;
    auto & relation_infos = set_relation_infos;

    JoinGraphStep::SetPlan plan;
    for (const auto & set : sets)
    {
        const bool is_all_relations = set.relations == all_relations;
        const auto set_columns = is_all_relations ? output_inputs : setColumns(set.relations);
        const auto & winner = recorded_splits->winners.at(set.relations);

        bool imprecise_estimate = false;
        for (size_t i = 0; i < leaf_imprecise.size(); ++i)
            if ((set.relations >> i) & 1)
                imprecise_estimate |= leaf_imprecise[i] || leaf_without_estimate[i];

        JoinGraphStep::SetPlan::Set planned_set{.relations = set.relations, .splits = {}};
        for (size_t index : set.splits)
        {
            const auto & split = recorded_splits->splits[index];
            const auto & left_header = headers.at(split.left);
            const auto & right_header = headers.at(split.right);

            auto predicates = splitPredicates(split);

            ActionsDAG::NodeMapping current_inputs;
            ActionsDAG::NodeRawConstPtrs required_output_nodes;
            for (UInt64 side : {split.left, split.right})
            {
                for (const auto * input : columns.at(side))
                {
                    current_inputs[input] = input;
                    required_output_nodes.push_back(input);
                }
            }
            for (const auto & predicate : predicates)
                required_output_nodes.push_back(predicate.getNode());
            if (is_all_relations)
                for (const auto * output : global_actions_dag->getOutputs())
                    required_output_nodes.push_back(output);

            JoinExpressionActions expression_actions(*left_header, *right_header,
                ActionsDAG::foldActionsByProjection(current_inputs, required_output_nodes));
            auto dag = expression_actions.getActionsDAG();
            auto & dag_outputs = dag->getOutputs();
            if (required_output_nodes.size() != dag_outputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output nodes size {} does not match current output nodes size in dag {}",
                    required_output_nodes.size(), dag->dumpDAG());
            auto mapping = std::ranges::to<ActionsDAG::NodeMapping>(std::views::zip(required_output_nodes, dag_outputs));

            JoinOperator join_operator(predicates.empty() ? JoinKind::Cross : JoinKind::Inner, JoinStrictness::All, JoinLocality::Unspecified);
            for (const auto & predicate : predicates)
                join_operator.expression.push_back(JoinActionRef(mapping.at(predicate.getNode()), expression_actions));

            dag_outputs.clear();
            ActionsDAG::NodeRawConstPtrs actions_after_join;
            for (const auto * input : set_columns)
                actions_after_join.push_back(mapping.at(input));
            if (is_all_relations)
            {
                for (const auto * output : global_actions_dag->getOutputs())
                {
                    const auto * mapped = mapping.at(output);
                    dag_outputs.push_back(mapped);
                    if (mapped->type == ActionsDAG::ActionType::COLUMN)
                        actions_after_join.push_back(mapped);
                }
                if (actions_after_join.empty())
                    actions_after_join.push_back(mapping.at(columns.at(split.left).front()));
            }
            else
            {
                dag_outputs = actions_after_join;
            }

            auto join_step = std::make_unique<JoinStepLogical>(
                left_header, right_header, std::move(join_operator), std::move(expression_actions), actions_after_join,
                join_settings, sorting_settings);
            join_step->setDisjunctionsOptimizationApplied(disjunctions_optimization_applied);
            join_step->setInputRelations(relation_infos.at(split.left), relation_infos.at(split.right));
            join_step->setJoinOrderEstimation(
                winner->estimated_rows, winner->column_stats, imprecise_estimate, winner->cost, winner->selectivity, cluster_id,
                winner->max_rows, winner->estimate_from_defaults);

            if (!headers.contains(set.relations))
            {
                headers[set.relations] = join_step->getOutputHeader();
                relation_infos[set.relations] = RelationEstimateInfo{
                    .name = join_step->getReadableRelationName(),
                    .estimated_rows = winner->estimated_rows,
                    .max_rows = winner->max_rows,
                    .imprecise_estimate = imprecise_estimate,
                    .estimate_from_defaults = winner->estimate_from_defaults,
                    .composite = true};
            }
            else if (!blocksHaveEqualStructure(*headers.at(set.relations), *join_step->getOutputHeader()))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Joins of one set of relations have different headers: {} and {}",
                    headers.at(set.relations)->dumpStructure(), join_step->getOutputHeader()->dumpStructure());
            }
            planned_set.splits.push_back({.left = split.left, .right = split.right, .step = std::move(join_step)});
        }
        columns[set.relations] = set_columns;
        ProfileEvents::increment(ProfileEvents::JoinEnumerationSplits, planned_set.splits.size());
        plan.sets.push_back(std::move(planned_set));
    }
    return plan;
}

}

/// A type whose equal values have equal bytes: a join compares it the same way as a hash key and as a value.
static bool comparesSameAsHashKey(const DataTypePtr & type)
{
    WhichDataType which(type);
    return which.isInteger() || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64() || which.isDecimal()
        || which.isEnum() || which.isUUID() || which.isIPv4() || which.isIPv6() || which.isString() || which.isFixedString();
}

/// Whether every join order of a set of the graph's relations gives the same rows when the predicates are placed by
/// `JoinOrderPlanBuilder::buildSetPlan`, so that Cascades may choose between all of them:
/// - a predicate may be applied at several joins, or at different joins in different orders, so it must be a filter
///   whose result and side effects do not depend on where and how often it runs; `arrayJoin` changes the number of
///   rows, so it is not such a filter;
/// - an equality is a hash key at a join whose two inputs give its two operands and a condition on values otherwise,
///   and join orders differ in that. The two compare floating-point values differently (NaN, -0 and 0), so the
///   operands must be of one type that both compare the same way, and each operand must come from one relation.
static bool canEnumerateJoinSplits(const QueryGraph & query_graph, const ActionsDAG & global_actions_dag, String & reason)
{
    if (query_graph.requires_conflict_detector)
        reason = "the graph needs a conflict detector";
    else if (global_actions_dag.hasArrayJoin())
        reason = "an expression has `arrayJoin`";
    else if (std::ranges::any_of(global_actions_dag.getNodes(), [](const auto & node) { return node.type == ActionsDAG::ActionType::PLACEHOLDER; }))
        reason = "an expression has a correlated column";
    else if (isSensitiveToEvaluationCount(global_actions_dag))
        reason = "an expression depends on how often it runs";
    if (!reason.empty())
        return false;

    for (const auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;
        auto [op, lhs, rhs] = edge.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        if (lhs.getSourceRelations().count() > 1 || rhs.getSourceRelations().count() > 1)
        {
            reason = fmt::format("an operand of `{}` depends on several relations", edge.getColumnName());
            return false;
        }
        auto lhs_type = removeLowCardinalityAndNullable(lhs.getType());
        auto rhs_type = removeLowCardinalityAndNullable(rhs.getType());
        if (!lhs_type->equals(*rhs_type) || !comparesSameAsHashKey(lhs_type))
        {
            reason = fmt::format("`{}` compares types {} and {}", edge.getColumnName(), lhs_type->getName(), rhs_type->getName());
            return false;
        }
    }
    return true;
}

static QueryPlan::Node chooseJoinOrder(QueryGraphBuilder query_graph_builder, QueryPlan::Nodes & nodes, JoinStrictness join_strictness)
{
    const bool disjunctions_optimization_applied = query_graph_builder.disjunctions_optimization_applied;

    QueryGraph query_graph;
    query_graph.relation_stats = std::move(query_graph_builder.relation_stats);
    query_graph.edges = std::move(query_graph_builder.join_edges);
    query_graph.edge_conflict_ops = std::move(query_graph_builder.join_edge_ops);
    query_graph.join_kinds = std::move(query_graph_builder.join_kinds);
    query_graph.outer_join_conditions = std::move(query_graph_builder.outer_join_conditions);
    query_graph.conflict_ops = std::move(query_graph_builder.conflict_ops);
    query_graph.semi_anti_flattened = query_graph_builder.context->allow_semi_anti_flattening;
    query_graph.join_strictness = join_strictness;
    query_graph.null_supplying_subtree_relations = std::move(query_graph_builder.null_supplying_subtree_relations);
    query_graph.cross_product_links = std::move(query_graph_builder.cross_product_links);
    query_graph.requires_conflict_detector = query_graph_builder.requires_conflict_detector
        || (query_graph.semi_anti_flattened
            && std::ranges::any_of(query_graph.conflict_ops, [](const auto & op) { return op.strictness != JoinStrictness::All; }));
    query_graph.probe_keys_within_preserved_side = query_graph_builder.probe_keys_within_preserved_side;
    for (size_t i = 0; i < query_graph_builder.inputs.size(); ++i)
    {
        if (typeid_cast<const JoinStepLogicalLookup *>(query_graph_builder.inputs[i]->step.get()))
            query_graph.prepared_storage_relations.set(i);
    }

    LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"), "Optimizing join order for query graph with {} relations", query_graph.relation_stats.size());

    std::unordered_map<BitSet, RelationEstimateInfo> leaf_relation_infos;
    Strings relations_without_statistics;
    Strings relations_estimated_from_data_lake_metadata;
    std::vector<UInt8> leaf_imprecise(query_graph.relation_stats.size());
    std::vector<UInt8> leaf_without_estimate(query_graph.relation_stats.size());
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        leaf_imprecise[i] = rel.imprecise_estimate;
        leaf_without_estimate[i] = !rel.estimated_rows;

        leaf_relation_infos[BitSet().set(i)] = RelationEstimateInfo{
            .name = rel.table_name.empty() ? fmt::format("R{}", i) : rel.table_name,
            .estimated_rows = rel.estimated_rows,
            .max_rows = rel.max_rows,
            .source = rel.source,
            .imprecise_estimate = rel.imprecise_estimate,
            .estimate_from_defaults = rel.estimate_from_defaults,
            .composite = rel.composite};

        if (isMissingStatisticsSource(rel.source))
            relations_without_statistics.push_back(rel.table_name.empty() ? fmt::format("table{}", i) : rel.table_name);
        else if (rel.source == RowEstimateSource::DataLakeMetadata && rel.imprecise_estimate)
            relations_estimated_from_data_lake_metadata.push_back(rel.table_name.empty() ? fmt::format("table{}", i) : rel.table_name);
    }

    /// `ALTER TABLE ... MATERIALIZE STATISTICS` does not apply to data lake tables, so they get their own message.
    if (!relations_estimated_from_data_lake_metadata.empty())
        LOG_DEBUG(
            getLogger("optimizeJoin"),
            "Join order optimization uses imprecise row count estimates derived from data lake metadata "
            "(for example Iceberg manifest files) for the following relation(s): {}. They have no column statistics "
            "for join reordering, so the chosen join order may be suboptimal",
            fmt::join(relations_estimated_from_data_lake_metadata, ", "));

    /// The listed names can be aliases or subquery labels, so no concrete `ALTER` command is suggested.
    if (!relations_without_statistics.empty())
        LOG_DEBUG(
            getLogger("optimizeJoin"),
            "Join order optimization uses imprecise row count estimates derived from the primary index "
            "because the following relation(s) have no column statistics available for join reordering: {}. "
            "The chosen join order may be suboptimal. Consider creating column statistics for the joined tables "
            "(for example, `ALTER TABLE <table> MATERIALIZE STATISTICS ALL`) and enabling the 'use_statistics' setting",
            fmt::join(relations_without_statistics, ", "));

    auto global_expression_actions = std::move(query_graph_builder.expression_actions);
    auto global_actions_dag = global_expression_actions.getActionsDAG();
    if (!global_actions_dag)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global expression actions DAG is not set");

    const auto & optimization_settings = query_graph_builder.context->optimization_settings;
    const UInt64 cluster_id = ++optimization_settings.join_reorder_next_cluster_id;

    /// Other complete orders for Cascades to choose from need the plain case: inner joins without
    /// columns whose type a join changes, and more than one way to split the relations.
    const bool inner_joins_only = query_graph.join_strictness == JoinStrictness::All && query_graph.join_kinds.empty()
        && query_graph.outer_join_conditions.empty() && query_graph.prepared_storage_relations.none()
        && std::ranges::all_of(query_graph_builder.type_changes, [](const auto & changes) { return changes.second.empty(); });
    if (optimization_settings.build_join_order_alternatives && inner_joins_only && query_graph.relation_stats.size() > 2)
        query_graph.max_join_order_alternatives = optimization_settings.cascades_join_order_alternatives;

    /// With `cascades_join_enumeration`, DPsub records the splits it accepts, and Cascades gets one memo group per set.
    const auto join_enumeration = optimization_settings.cascades_join_enumeration;
    std::shared_ptr<RecordedJoinSplits> recorded_splits;
    if (join_enumeration != QueryPlanOptimizationSettings::JoinEnumeration::Off && optimization_settings.build_join_order_alternatives
        && inner_joins_only && query_graph.relation_stats.size() > 2)
    {
        String reason;
        if (canEnumerateJoinSplits(query_graph, *global_actions_dag, reason))
        {
            recorded_splits = std::make_shared<RecordedJoinSplits>();
            query_graph.recorded_splits = recorded_splits;
        }
        else
            LOG_DEBUG(getLogger("optimizeJoin"), "Cascades gets the join orders of the graph, not its sets: {}", reason);
    }

    const auto join_order = optimizeJoinOrder(std::move(query_graph), optimization_settings);

    if (!query_graph_builder.context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryGraphBuilder context is not set");

    auto join_settings = std::move(query_graph_builder.context->join_settings);
    auto sorting_settings = std::move(query_graph_builder.context->sorting_settings);

    /// After applying OUTER joins some columns may change their types (in case of join_use_nulls or JOIN USING)
    /// We need to track this change, this map tracks input columns and how they are transformed
    /// Each next step uses mapped columns as inputs

    /// Input in global dag -> it's position
    std::unordered_map<const ActionsDAG::Node *, size_t> input_node_map;

    /// input_position -> (relation no, input)
    std::vector<std::pair<size_t, const ActionsDAG::Node *>> initial_input_nodes;

    const auto & global_inputs = global_actions_dag->getInputs();
    for (size_t input_idx = 0; input_idx < global_inputs.size(); ++input_idx)
    {
        const auto * input = global_inputs[input_idx];
        auto src_rels = JoinActionRef(input, global_expression_actions).getSourceRelations();
        auto rel_idx = src_rels.getSingleBit();
        if (!rel_idx.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input node {} has {} source relations, expected 1", input->result_name, toString(src_rels));

        initial_input_nodes.emplace_back(rel_idx.value(), input);
        input_node_map[input] = input_idx;
    }


    auto builder = std::make_shared<JoinOrderPlanBuilder>(std::move(join_settings), std::move(sorting_settings));

    builder->query_graph_builder = std::move(query_graph_builder);
    builder->global_expression_actions = std::move(global_expression_actions);
    builder->global_actions_dag = std::move(global_actions_dag);
    builder->input_node_map = std::move(input_node_map);
    builder->initial_input_nodes = std::move(initial_input_nodes);
    builder->leaf_relation_infos = std::move(leaf_relation_infos);
    builder->leaf_imprecise = std::move(leaf_imprecise);
    builder->leaf_without_estimate = std::move(leaf_without_estimate);
    builder->join_strictness = join_strictness;
    builder->cluster_id = cluster_id;
    builder->disjunctions_optimization_applied = disjunctions_optimization_applied;

    const auto & leaf_input_nodes = builder->query_graph_builder.inputs;
    builder->chosen_order = join_order.best;
    QueryPlan::Node * best_root = builder->build(join_order.best, leaf_input_nodes, nodes);
    builder->other_orders = join_order.alternatives;

    bool import_sets = false;
    if (recorded_splits)
    {
        builder->recorded_splits = recorded_splits;
        String reason;
        import_sets = builder->selectSetSplits(
            join_enumeration, optimization_settings.cascades_join_enumeration_max_splits,
            optimization_settings.cascades_join_order_alternatives + 1, reason);
        if (import_sets)
            LOG_DEBUG(getLogger("optimizeJoin"), "Cascades gets {} sets of the join graph from {} accepted of {} considered splits",
                builder->set_splits.size(), recorded_splits->splits.size(), recorded_splits->considered_splits);
        else
            LOG_DEBUG(getLogger("optimizeJoin"), "Cascades gets the join orders of the graph, not its sets: {}", reason);
    }

    /// Cascades chooses between the orders: the graph becomes one step over its inputs, and the plans of the other
    /// orders, or the joins of its sets, are built when Cascades imports it, see `JoinGraphStep`.
    if (!join_order.alternatives.empty() || import_sets)
    {
        SharedHeaders input_headers;
        for (const auto * input : leaf_input_nodes)
            input_headers.push_back(input->step->getOutputHeader());
        QueryPlan::Node result;
        result.children = leaf_input_nodes;
        result.cost_estimation = best_root->cost_estimation;
        result.step = std::make_unique<JoinGraphStep>(std::move(input_headers), best_root, builder);
        return result;
    }

    /// Return the root by value and remove it from the nodes list.
    /// Caller may put the node back into the list if needed or replace existing node
    auto it = std::ranges::find_if(nodes, [&](const QueryPlan::Node & candidate) { return &candidate == best_root; });
    if (it == nodes.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join tree root is not in the plan nodes");
    auto taken = std::move(*it);
    nodes.erase(it);
    return taken;
}

static void collectJoinGraphRelationHeaders(
    const QueryPlan::Node * node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children,
    bool below_null_supplying);

/// Mirrors `buildQueryGraph` for a single join node: collects the output headers of the relations
/// that the join order optimizer would produce for it (descending into flattenable child joins).
/// `allow_semi_anti_children` must match `addChildQueryGraph`'s flatten gate so this header shadow
/// stays in sync when CD-A semi/anti reordering is enabled.
static void collectJoinGraphRelationHeadersForJoin(
    const QueryPlan::Node & join_node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children,
    bool below_null_supplying = false)
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(join_node.step.get());
    if (!join_step || join_node.children.size() != 2)
    {
        relation_headers.push_back(join_node.step->getOutputHeader());
        return;
    }

    const auto join_kind = join_step->getJoinOperator().kind;
    const auto type_changing_sides = join_step->typeChangingSides();

    const bool allow_left_subgraph = !type_changing_sides.contains(JoinTableSide::Left)
        && isFlattenableJoinSide(join_kind, JoinTableSide::Left, allow_semi_anti_children);
    const size_t lhs_before = relation_headers.size();
    collectJoinGraphRelationHeaders(
        join_node.children[0], allow_left_subgraph ? join_steps_limit - 1 : 0, join_settings, merge_expression_into_join, relation_headers,
        allow_semi_anti_children, below_null_supplying || isNullSupplyingSide(join_kind, JoinTableSide::Left));
    const size_t lhs_count = relation_headers.size() - lhs_before;

    const bool allow_right_subgraph = !type_changing_sides.contains(JoinTableSide::Right)
        && isFlattenableJoinSide(join_kind, JoinTableSide::Right, allow_semi_anti_children);
    collectJoinGraphRelationHeaders(
        join_node.children[1], allow_right_subgraph ? static_cast<int>(join_steps_limit - lhs_count) : 0, join_settings, merge_expression_into_join, relation_headers,
        allow_semi_anti_children, below_null_supplying || isNullSupplyingSide(join_kind, JoinTableSide::Right));
}

/// Mirrors `addChildQueryGraph`: either flattens a child join into multiple relations, or treats
/// the (possibly expression-step-peeled) node as a single relation and records its output header.
static void collectJoinGraphRelationHeaders(
    const QueryPlan::Node * node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    std::vector<SharedHeader> & relation_headers,
    bool allow_semi_anti_children,
    bool below_null_supplying)
{
    /// Peeling must match `addChildQueryGraph`, see `canFlattenThroughExpression`.
    const auto * effective = node;
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(effective->step.get());
        expression_step && effective->children.size() == 1
        && canFlattenThroughExpression(expression_step->getExpression(), merge_expression_into_join, below_null_supplying))
    {
        effective = effective->children[0];
    }

    if (const auto * child_join_step = typeid_cast<const JoinStepLogical *>(effective->step.get());
        child_join_step && !child_join_step->isOptimized())
    {
        /// Keep in sync with `addChildQueryGraph`.
        const bool allow_child_join_kind = isFlattenableChildJoin(
                child_join_step->getJoinOperator().kind, child_join_step->getJoinOperator().strictness, allow_semi_anti_children)
            && child_join_step->typeChangingSides().empty();

        if (child_join_step->getJoinSettings() == join_settings && join_steps_limit > 1 && allow_child_join_kind)
        {
            collectJoinGraphRelationHeadersForJoin(
                *effective, join_steps_limit, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children,
                below_null_supplying);
            return;
        }
    }

    relation_headers.push_back(node->step->getOutputHeader());
}

/// The join order optimizer reconstructs join steps using `JoinExpressionActions`, which requires
/// column names to be unique across the two sides of every reconstructed join. When two relations in
/// the join graph share a column name, reordering can place them on opposite sides of a reconstructed
/// join and hit a `LOGICAL_ERROR`. This can happen, for example, when parallel replicas or distributed
/// query rewriting reuses table identifiers (e.g. both relations produce `__table1`), or when scalar
/// subquery results collide with join table aliases. The unoptimized join handles such names
/// correctly, so detect this up front and skip the reordering in that case.
static bool joinGraphHasOverlappingColumnNames(
    const QueryPlan::Node & join_node,
    int join_steps_limit,
    const JoinSettings & join_settings,
    bool merge_expression_into_join,
    bool allow_semi_anti_children)
{
    std::vector<SharedHeader> relation_headers;
    collectJoinGraphRelationHeadersForJoin(join_node, join_steps_limit, join_settings, merge_expression_into_join, relation_headers, allow_semi_anti_children);

    std::unordered_set<std::string_view> seen_names;
    for (const auto & header : relation_headers)
    {
        if (!header)
            continue;
        for (const auto & column : *header)
        {
            if (!seen_names.insert(column.name).second)
                return true;
        }
    }
    return false;
}

void optimizeJoinLogical(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return;

    if (node.children.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStepLogical should have exactly 2 children, but has {}", node.children.size());

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinOptimizeMicroseconds);
    ensureJoinEstimated(*join_step, node, nodes, optimization_settings);
}

void optimizeJoinLogicalImpl(JoinStepLogical * join_step, QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    for (auto * child : node.children)
    {
        if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
            lookup_step->optimize(optimization_settings);
    }

    const auto & join_operator = join_step->getJoinOperator();
    auto strictness = join_operator.strictness;
    auto kind = join_operator.kind;
    auto locality = join_operator.locality;
    if (!optimization_settings.query_plan_optimize_join_order_limit
        || (strictness != JoinStrictness::All && !isSwapOnlyJoinStrictness(strictness))
        || locality != JoinLocality::Unspecified
        || kind == JoinKind::Paste
        || !join_operator.residual_filter.empty()
    )
    {
        estimateJoinInPlace(*join_step, node, nodes, optimization_settings);
        return;
    }

    int query_graph_size_limit = safe_cast<int>(optimization_settings.query_plan_optimize_join_order_limit);

    /// With a conflict detector (CD) the group of tables is built for DPsub: semi/anti and outer joins
    /// come in with both of their sides (see `isFlattenableJoinSide`). Only DPsub can plan such a
    /// group, and it turns down more tables than `DPSUB_MAX_RELATIONS`. So count the tables the group
    /// would end up with, and build it the ordinary way when there are too many: greedy can then take
    /// it over, whole, instead of it being cut down to a size DPsub accepts.
    bool reorder_semi_anti = conflictDetectorReordersSemiAnti(optimization_settings);
    if (reorder_semi_anti)
    {
        std::vector<SharedHeader> flattened_relations;
        collectJoinGraphRelationHeadersForJoin(
            node, query_graph_size_limit, join_step->getJoinSettings(),
            optimization_settings.merge_expression_into_join, flattened_relations, /*allow_semi_anti_children=*/ true);
        reorder_semi_anti = flattened_relations.size() <= DPSUB_MAX_RELATIONS;
    }

    /// For CD, semi/anti and `FULL` joins are fully reorderable rather than swap-only, so they keep the
    /// full graph size limit. The `ANY` strictness stays capped, as CD does not model it.
    const bool cd_reorders_top = reorder_semi_anti
        && (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti || isFull(kind));
    if ((isSwapOnlyJoinStrictness(strictness) || isSwapOnlyJoinKind(kind)) && query_graph_size_limit > 2 && !cd_reorders_top)
        /// Do not reorder joins, only allow swap
        query_graph_size_limit = 2;

    /// Skip join order optimization when the join graph contains relations with overlapping column
    /// names, which the `JoinExpressionActions`-based reconstruction does not support. See the comment
    /// on `joinGraphHasOverlappingColumnNames`. This generalizes a check over the immediate children to
    /// the whole flattened relation set, so it also covers overlaps that only appear after flattening.
    if (joinGraphHasOverlappingColumnNames(
            node, query_graph_size_limit, join_step->getJoinSettings(),
            optimization_settings.merge_expression_into_join, reorder_semi_anti))
    {
        join_step->setOptimized();
        return;
    }

    QueryGraphBuilder query_graph_builder(optimization_settings, node, join_step->getJoinSettings(), join_step->getSortingSettings());
    query_graph_builder.context->stats_hint = join_step->getTableStatsHint();
    /// A swap-only semi or anti join keeps its graph to itself, so the property holds for the graph.
    query_graph_builder.probe_keys_within_preserved_side = query_graph_size_limit == 2 && join_step->probeKeysWithinPreservedSide();
    query_graph_builder.context->allow_semi_anti_flattening = reorder_semi_anti;

    buildQueryGraph(query_graph_builder, node, nodes, query_graph_size_limit);
    node = chooseJoinOrder(std::move(query_graph_builder), nodes, strictness);
}

}

}
