#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <memory>
#include <stack>

namespace DB
{

#if defined(DEBUG_OR_SANITIZER_BUILD)
namespace
{
void checkHeaders(const QueryPlan::Node & node, const std::string_view context_description, const size_t check_depth)
{
    if (check_depth == 0)
        return;

    const auto & parent_headers = node.step->getInputHeaders();
    for (auto child_id = 0U; child_id < node.children.size(); ++child_id)
    {
        const auto & child_node = *node.children[child_id];
        assertBlocksHaveEqualStructure(*parent_headers[child_id], *child_node.step->getOutputHeader(), context_description);
        checkHeaders(child_node, context_description, check_depth - 1);
    }
}
}
#endif

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TOO_MANY_QUERY_PLAN_OPTIMIZATIONS;
extern const int PROJECTION_NOT_USED;
extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

void optimizeTreeFirstPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!optimization_settings.optimize_plan)
        return;

    struct Frame
    {
        QueryPlan::Node * node = nullptr;

        /// If not zero, traverse only depth_limit layers of tree (if no other optimizations happen).
        /// Otherwise, traverse all children.
        size_t depth_limit = 0;

        /// Next child to process.
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push({.node = &root});

    const size_t max_optimizations_to_apply = optimization_settings.max_optimizations_to_apply;
    size_t total_applied_optimizations = 0;


    Optimization::ExtraSettings extra_settings = {
        optimization_settings.max_step_description_length,
        optimization_settings.max_limit_for_vector_search_queries,
        optimization_settings.vector_search_with_rescoring,
        optimization_settings.vector_search_filter_strategy,
        optimization_settings.use_index_for_in_with_subqueries_max_values,
        optimization_settings.network_transfer_limits,
        optimization_settings.use_skip_indexes_for_top_k,
        optimization_settings.use_top_k_dynamic_filtering,
        optimization_settings.max_limit_for_top_k_optimization,
        optimization_settings.use_skip_indexes_on_data_read,
        optimization_settings.parallel_replicas_filter_pushdown,
    };

    while (!stack.empty())
    {
        auto & frame = stack.top();

        /// If traverse_depth_limit == 0, then traverse without limit (first entrance)
        /// If traverse_depth_limit > 1, then traverse with (limit - 1)
        if (frame.depth_limit != 1)
        {
            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                stack.push({
                    .node = frame.node->children[frame.next_child],
                    .depth_limit = frame.depth_limit ? (frame.depth_limit - 1) : 0,
                });

                ++frame.next_child;
                continue;
            }
        }

        size_t max_update_depth = 0;

        /// Apply all optimizations.
        for (const auto & optimization : getOptimizations())
        {
            if (!(optimization_settings.*(optimization.is_enabled)))
                continue;

            /// Just in case, skip optimization if it is not initialized.
            if (!optimization.apply)
                continue;

            if (max_optimizations_to_apply && max_optimizations_to_apply < total_applied_optimizations)
            {
                if (optimization_settings.is_explain)
                    return;

                throw Exception(
                    ErrorCodes::TOO_MANY_QUERY_PLAN_OPTIMIZATIONS,
                    "Too many optimizations applied to query plan. Current limit {}",
                    max_optimizations_to_apply);
            }


            /// Try to apply optimization.
            auto update_depth = optimization.apply(frame.node, nodes, extra_settings);
            if (update_depth)
            {
#if defined(DEBUG_OR_SANITIZER_BUILD)
                checkHeaders(*frame.node, String("after optimization ") + optimization.name, update_depth);
#endif
                ++total_applied_optimizations;
            }
            max_update_depth = std::max<size_t>(max_update_depth, update_depth);
        }

        /// Traverse `max_update_depth` layers of tree again.
        if (max_update_depth)
        {
            frame.depth_limit = max_update_depth;
            frame.next_child = 0;
            continue;
        }

        /// Nothing was applied.
        stack.pop();
    }
}

struct NoOp
{
};

template <typename Func1, typename Func2 = NoOp>
void traverseQueryPlan(Stack & stack, QueryPlan::Node & root, Func1 && on_enter, Func2 && on_leave = {})
{
    stack.clear();
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if constexpr (!std::is_same_v<Func1, NoOp>)
        {
            if (frame.next_child == 0)
            {
                on_enter(*frame.node);
            }
        }

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        if constexpr (!std::is_same_v<Func2, NoOp>)
        {
            on_leave(*frame.node);
        }

        stack.pop_back();
    }
}

void tryMakeDistributedJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
void tryMakeDistributedAggregation(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
void tryMakeDistributedSorting(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
void tryMakeDistributedRead(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings);
void optimizeExchanges(QueryPlan::Node & root);

/// Find the top node of the parallel replicas plan. E.g.:
///
/// Expression ((Project names + Projection))
///  MergingAggregated
///    Union
///      Aggregating  <-- this node is the last plan step to be executed on replicas
///        Expression (Before GROUP BY)
///          Expression ((WHERE + Change column names to column identifiers))
///            ReadFromMergeTree (default.hits)
///      ReadFromRemoteParallelReplicas (Query: ... Replicas: ...)
///
static QueryPlan::Node * findTopNodeOfReplicasPlan(QueryPlan::Node * plan_with_parallel_replicas_root)
{
    QueryPlan::Node * replicas_plan_top_node = nullptr;

    Stack stack;
    stack.push_back({.node = plan_with_parallel_replicas_root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Currently the approach is very simple: we look for Union step in the plan tree,
        /// and consider its children. The first child that is not ReadFromParallelRemoteReplicas
        /// is considered the top node of replicas plan.
        if (typeid_cast<UnionStep *>(frame.node->step.get()))
        {
            bool found_read_from_parallel_replicas = false;

            for (const auto & child : frame.node->children)
            {
                auto * node = child;
                /// ExpressionStep can be placed on top of ReadFromRemoteParallelReplicas
                if (typeid_cast<const ExpressionStep *>(node->step.get()) || typeid_cast<const FilterStep *>(node->step.get()))
                {
                    chassert(!node->children.empty());
                    node = node->children.front();
                }
                if (typeid_cast<const DelayedCreatingSetsStep *>(node->step.get())
                    || typeid_cast<const CreatingSetsStep *>(node->step.get()))
                {
                    chassert(!node->children.empty());
                    node = node->children.front();
                }
                if (!typeid_cast<const ReadFromParallelRemoteReplicasStep *>(node->step.get()))
                {
                    if (replicas_plan_top_node)
                    {
                        // TODO(nickitat): support multiple read steps with parallel replicas
                        LOG_DEBUG(getLogger("optimizeTree"), "Top node for parallel replicas plan is already found");
                        return nullptr;
                    }

                    replicas_plan_top_node = node;
                }
                else
                {
                    found_read_from_parallel_replicas = true;
                }
            }

            /// We found pattern
            ///     Union
            ///       ReadFromParallelRemoteReplicas
            ///       <replicas_plan_top_node>
            if (replicas_plan_top_node && found_read_from_parallel_replicas)
                break;
        }

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    return replicas_plan_top_node;
}

/// Now when we found the top node of replicas plan, we need to find the corresponding node in the single node plan.
/// The working principle behind automatic parallel replicas is that we use statistics collected during execution of single-node plan
/// to estimate whether parallel replicas will be beneficial for the query or not. For that, we need to estimate how much data
/// replicas will send to the initiator. To do that, we found the node that will be at the top of replicas plan (e.g. Aggregating step in the example above),
/// and ask it collect statistics on the number of bytes it'd send to the initiator if we executed the query with parallel replicas.
static std::pair<const QueryPlan::Node *, size_t> findCorrespondingNodeInSingleNodePlan(
    const QueryPlan::Node & final_node_in_replica_plan,
    QueryPlan::Node & parallel_replicas_plan_root,
    QueryPlan::Node & single_replica_plan_root)
{
    auto pr_node_hashes = calculateHashTableCacheKeys(parallel_replicas_plan_root);
    if (auto it = pr_node_hashes.find(&final_node_in_replica_plan); it != pr_node_hashes.end())
    {
        auto nopr_node_hashes = calculateHashTableCacheKeys(single_replica_plan_root);

        for (const auto & [nopr_node, nopr_hash] : nopr_node_hashes)
        {
            if (nopr_hash == it->second)
            {
                if (!nopr_node->step->supportsDataflowStatisticsCollection())
                {
                    LOG_DEBUG(
                        getLogger("optimizeTree"),
                        "Step ({}) doesn't support dataflow statistics collection. Skipping statistics collection",
                        nopr_node->step->getName());
                    return std::make_pair(nullptr, 0);
                }

                LOG_DEBUG(getLogger("optimizeTree"), "Found matching node in original plan: {}", nopr_node->step->getName());
                return std::make_pair(nopr_node, nopr_hash);
            }
        }
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot find step with matching hash in single-node plan");
        return std::make_pair(nullptr, 0);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find replicas_plan_top_node in hash table");
    }
}

static ReadFromMergeTree * findReadingStep(const QueryPlan::Node & top_of_single_replica_plan)
{
    const auto * reading_step = &top_of_single_replica_plan;
    while (reading_step && !reading_step->children.empty())
    {
        // TODO(nickitat): support multiple read steps with parallel replicas
        const auto * lazy_joining = typeid_cast<const JoinLazyColumnsStep *>(reading_step->step.get());

        if (!lazy_joining && reading_step->children.size() > 1)
            return nullptr;
        reading_step = reading_step->children.front();
    }

    chassert(reading_step);
    if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(reading_step->step.get()))
        return read_from_merge_tree;

    LOG_DEBUG(
        getLogger("optimizeTree"),
        "Cannot find ReadFromMergeTree step in single-replica plan (found {}). Skipping optimization",
        reading_step->step->getName());
    return nullptr;
}

/// Transplant the sets from the single-replica plan to the parallel-replicas plan once we decided to enable parallel replicas
static void moveSetsFromLocalPlanToReplicasPlan(const QueryPlan & single_replica_plan, const QueryPlan & parallel_replicas_plan)
{
    Stack stack;
    std::map<FutureSet::Hash, SetAndKeyPtr> sets_map;

    // Create a map: set_key -> set
    stack.clear();
    traverseQueryPlan(
        stack,
        *single_replica_plan.getRootNode(),
        [&](auto & frame_node)
        {
            if (auto * creating_sets_step = typeid_cast<DelayedCreatingSetsStep *>(frame_node.step.get()))
            {
                const auto sets = creating_sets_step->detachSets();
                for (const auto & future_set : sets)
                {
                    if (auto set = future_set->detachSetAndKey())
                        sets_map[future_set->getHash()] = std::move(set);
                }
            }
        });

    // Now transplant the sets
    stack.clear();
    traverseQueryPlan(
        stack,
        *parallel_replicas_plan.getRootNode(),
        [&](auto & frame_node)
        {
            if (const auto * creating_sets_step = typeid_cast<DelayedCreatingSetsStep *>(frame_node.step.get()))
            {
                for (const auto & future_set : creating_sets_step->getSets())
                {
                    if (auto it = sets_map.find(future_set->getHash()); it != sets_map.end())
                    {
                        future_set->replaceSetAndKey(it->second);
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Cannot find a matching set in the map of sets from single-replica plan");
                    }
                }
            }
        });
}

/// Heuristic-based algorithm to decide whether to enable parallel replicas for the given query
static void considerEnablingParallelReplicas(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan & query_plan)
{
    if (!optimization_settings.automatic_parallel_replicas_mode
        || !optimization_settings.query_plan_with_parallel_replicas_builder
        || optimization_settings.parallel_replicas_enabled)
        return;

    // Cannot guarantee projection usage with parallel replicas
    if (optimization_settings.force_use_projection)
        return;

    Stack stack;
    // Technically, it isn't required for all steps to support dataflow statistics collection,
    // but only for those that we will actually instrument (see `setRuntimeDataflowStatisticsCacheUpdater` calls below).
    // However, currently only relatively simple plans are supported (no JOINs, CreatingSets from subqueries, UNIONs, etc.),
    // since all these steps obviously don't support statistics collection, `supportsDataflowStatisticsCollection` is handy to check if the plan is simple enough.
    bool plan_is_simple_enough = true;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            plan_is_simple_enough &= frame_node.step->supportsDataflowStatisticsCollection()
                || typeid_cast<const DelayedCreatingSetsStep *>(frame_node.step.get())
                || typeid_cast<const CreatingSetsStep *>(frame_node.step.get());
        });
    if (!plan_is_simple_enough)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Some steps in the plan don't support dataflow statistics collection. Skipping optimization");
        return;
    }

    auto plan_with_parallel_replicas = optimization_settings.query_plan_with_parallel_replicas_builder();
    if (!plan_with_parallel_replicas)
        return;

    const auto * final_node_in_replica_plan = findTopNodeOfReplicasPlan(plan_with_parallel_replicas->getRootNode());
    if (!final_node_in_replica_plan)
        return;
    LOG_DEBUG(getLogger("optimizeTree"), "Top node of replicas plan: {}", final_node_in_replica_plan->step->getName());

    const auto [corresponding_node_in_single_replica_plan, single_replica_plan_node_hash]
        = findCorrespondingNodeInSingleNodePlan(*final_node_in_replica_plan, *plan_with_parallel_replicas->getRootNode(), root);
    if (!corresponding_node_in_single_replica_plan)
        return;

    /// Now we need to identify the reading step that should be instrumented for statistics collection
    ReadFromMergeTree * source_reading_step = findReadingStep(*corresponding_node_in_single_replica_plan);
    if (!source_reading_step)
        return;

    const auto analysis
        = source_reading_step->getAnalyzedResult() ? source_reading_step->getAnalyzedResult() : source_reading_step->selectRangesToRead();
    if (!analysis)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot get index analysis result from MergeTree table. Skipping optimization");
        return;
    }
    const auto rows_to_read = analysis->selected_rows;
    if (!rows_to_read)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Index analysis result doesn't contain selected rows. Skipping optimization");
        return;
    }

    bool table_data_drifted_significantly = true;

    const auto & stats_cache = getRuntimeDataflowStatisticsCache();
    if (const auto stats = stats_cache.getStats(single_replica_plan_node_hash))
    {
        bool apply_plan_with_parallel_replicas = optimization_settings.automatic_parallel_replicas_mode != 2;
        if (std::max<size_t>(stats->total_rows_to_read, rows_to_read) > std::min<size_t>(stats->total_rows_to_read, rows_to_read) * 2)
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Significant difference in total rows from storage detected (previously {}, now {}). Recollecting statistics",
                stats->total_rows_to_read,
                rows_to_read);
            apply_plan_with_parallel_replicas = false;
        }
        else
        {
            table_data_drifted_significantly = false;
        }

        if (apply_plan_with_parallel_replicas)
        {
            const auto max_threads = optimization_settings.max_threads;
            const auto num_replicas = optimization_settings.max_parallel_replicas;
            const auto local_plan_cost_estimation = stats->input_bytes / max_threads;
            const auto replicas_plan_cost_estimation
                = (stats->input_bytes / (max_threads * num_replicas)) + stats->output_bytes / num_replicas;
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "The applied formula: {} / {} ? ({} / ({} * {}) + {} / {}) ≡ {} ? {}",
                stats->input_bytes,
                max_threads,
                stats->input_bytes,
                max_threads,
                num_replicas,
                stats->output_bytes,
                num_replicas,
                local_plan_cost_estimation,
                replicas_plan_cost_estimation);
            if (local_plan_cost_estimation > replicas_plan_cost_estimation)
            {
                if (optimization_settings.automatic_parallel_replicas_min_bytes_per_replica
                    && stats->input_bytes / num_replicas < optimization_settings.automatic_parallel_replicas_min_bytes_per_replica)
                {
                    LOG_DEBUG(
                        getLogger("optimizeTree"),
                        "Not enabling parallel replicas reading because {} < automatic_parallel_replicas_min_bytes_per_replica {}",
                        stats->input_bytes / num_replicas,
                        optimization_settings.automatic_parallel_replicas_min_bytes_per_replica);
                    return;
                }

                ReadFromMergeTree * local_replica_plan_reading_step = findReadingStep(*final_node_in_replica_plan);
                if (!local_replica_plan_reading_step)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find ReadFromMergeTree step in local parallel replicas plan");
                chassert(local_replica_plan_reading_step->getAnalyzedResult() == nullptr);
                local_replica_plan_reading_step->setAnalyzedResult(analysis);
                moveSetsFromLocalPlanToReplicasPlan(query_plan, *plan_with_parallel_replicas);
                query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*plan_with_parallel_replicas));
                return;
            }
        }
    }
    else
    {
        LOG_DEBUG(getLogger("optimizeTree"), "No stats found for hash {}", single_replica_plan_node_hash);
    }

    if (table_data_drifted_significantly
        || optimization_settings.automatic_parallel_replicas_mode == 2 // automatic_parallel_replicas_mode == 2 enforces statistics recollection
    )
    {
        auto updater = std::make_shared<RuntimeDataflowStatisticsCacheUpdater>(single_replica_plan_node_hash, rows_to_read);
        source_reading_step->setRuntimeDataflowStatisticsCacheUpdater(updater);
        corresponding_node_in_single_replica_plan->step->setRuntimeDataflowStatisticsCacheUpdater(updater);
    }
}


void optimizeTreeSecondPass(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes, QueryPlan & query_plan)
{
    const size_t max_optimizations_to_apply = optimization_settings.max_optimizations_to_apply;
    std::unordered_set<String> applied_projection_names;
    bool has_reading_from_mt = false;

    Optimization::ExtraSettings extra_settings = {
        optimization_settings.max_step_description_length,
        optimization_settings.max_limit_for_vector_search_queries,
        optimization_settings.vector_search_with_rescoring,
        optimization_settings.vector_search_filter_strategy,
        optimization_settings.use_index_for_in_with_subqueries_max_values,
        optimization_settings.network_transfer_limits,
        optimization_settings.use_skip_indexes_for_top_k,
        optimization_settings.use_top_k_dynamic_filtering,
        optimization_settings.max_limit_for_top_k_optimization,
        optimization_settings.use_skip_indexes_on_data_read,
        optimization_settings.parallel_replicas_filter_pushdown,
    };

    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        if (optimization_settings.query_plan_optimize_primary_key)
            optimizePrimaryKeyConditionAndLimit(stack);

        updateQueryConditionCache(stack, optimization_settings);

        /// Must be executed after index analysis and before PREWHERE optimization.
        processAndOptimizeTextIndexFunctions(stack, nodes, optimization_settings.direct_read_from_text_index);

        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    if (!optimization_settings.correlated_subqueries_use_in_memory_buffer)
    {
        /// Materialize subplan references before other optimizations.
        traverseQueryPlan(stack, root, [&](auto & frame_node)
        {
            materializeQueryPlanReferences(frame_node, nodes);
        });

        /// Remove CommonSubplanSteps (they must be not used at that point).
        traverseQueryPlan(stack, root, [&](auto & frame_node)
        {
            optimizeUnusedCommonSubplans(frame_node);
        });
    }

    bool join_runtime_filters_were_added = false;
    traverseQueryPlan(stack, root,
        [&](auto & frame_node)
        {
            optimizeJoinLogical(frame_node, nodes, optimization_settings);
            optimizeJoinLegacy(frame_node, nodes, optimization_settings);
            useMemoryBufferForCommonSubplanResult(frame_node, optimization_settings);
        },
        [&](auto & frame_node)
        {
            if (optimization_settings.enable_join_runtime_filters)
                join_runtime_filters_were_added |= tryAddJoinRuntimeFilter(frame_node, nodes, optimization_settings);
            convertLogicalJoinToPhysical(frame_node, nodes, optimization_settings);
        });

    /// If join runtime filters were added re-run push down optimizations
    /// to move newly added runtime filter as deep in the tree as possible
    if (join_runtime_filters_were_added)
    {
        traverseQueryPlan(stack, root,
            [&](auto & frame_node)
            {
                /// If there are multiple Expression nodes below Filter node then we need to repeat merging Filter and Expression
                while (true)
                {
                    size_t changed_nodes = 0;
                    changed_nodes += tryMergeExpressions(&frame_node, nodes, {});
                    changed_nodes += tryMergeFilters(&frame_node, nodes, {});
                    changed_nodes += tryPushDownFilter(&frame_node, nodes, {});

                    if (!changed_nodes)
                        break;
                }
            });
    }

    /// Do PREWHERE optimization after all possible filters including JOIN runtime filters were pushed down
    if (optimization_settings.optimize_prewhere)
    {
        traverseQueryPlan(stack, root,
            [&](auto & frame_node)
            {
                optimizePrewhere(frame_node, optimization_settings.remove_unused_columns);
            });
    }

    traverseQueryPlan(stack, root,
        [&](auto & frame_node)
        {
            if (optimization_settings.read_in_order)
                optimizeReadInOrder(frame_node, nodes, optimization_settings);

            if (optimization_settings.distinct_in_order)
                optimizeDistinctInOrder(frame_node, nodes, optimization_settings);
        },
        [&](auto & frame_node)
        {
            /// After all children were processed, try to apply distributed read, join and aggregation optimizations.
            if (optimization_settings.make_distributed_plan)
            {
                tryMakeDistributedJoin(frame_node, nodes, optimization_settings);
                tryMakeDistributedAggregation(frame_node, nodes, optimization_settings);
                tryMakeDistributedSorting(frame_node, nodes, optimization_settings);
                tryMakeDistributedRead(frame_node, nodes, optimization_settings);
            }
        });

    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        {
            /// NOTE: frame cannot be safely used after stack was modified.
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                has_reading_from_mt |= typeid_cast<const ReadFromMergeTree *>(frame.node->step.get()) != nullptr;

                /// Projection optimization relies on PK optimization
                if (optimization_settings.optimize_projection)
                {
                    auto applied_projection = optimizeUseAggregateProjections(
                        *frame.node,
                        nodes,
                        optimization_settings.optimize_use_implicit_projections,
                        optimization_settings.is_parallel_replicas_initiator_with_projection_support,
                        optimization_settings.max_step_description_length);
                    if (applied_projection)
                        applied_projection_names.insert(*applied_projection);
                }

                if (optimization_settings.aggregation_in_order)
                    optimizeAggregationInOrder(*frame.node, nodes, optimization_settings);
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }
        }

        if (optimization_settings.optimize_projection)
        {
            /// Projection optimization relies on PK optimization
            if (auto applied_projection = optimizeUseNormalProjections(
                stack,
                nodes,
                optimization_settings.is_parallel_replicas_initiator_with_projection_support,
                optimization_settings.max_step_description_length))
            {
                applied_projection_names.insert(*applied_projection);

                if (max_optimizations_to_apply && max_optimizations_to_apply < applied_projection_names.size())
                {
                    /// Limit only first pass in EXPLAIN mode.
                    if (!optimization_settings.is_explain)
                        throw Exception(
                            ErrorCodes::TOO_MANY_QUERY_PLAN_OPTIMIZATIONS,
                            "Too many projection optimizations applied to query plan. Current limit {}",
                            max_optimizations_to_apply);
                }

                /// Stack is updated after this optimization and frame is not valid anymore.
                /// Try to apply optimizations again to newly added plan steps.
                --stack.back().next_child;
                continue;
            }
        }

        stack.pop_back();
    }

    /// Find ReadFromLocalParallelReplicaStep and replace with optimized local plan.
    /// Place it after projection optimization to avoid executing projection optimization twice in the local plan,
    /// Which would cause an exception when force_use_projection is enabled.
    bool read_from_local_parallel_replica_plan = false;
    stack.push_back({.node = &root});
    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }
        if (auto * read_from_local = typeid_cast<ReadFromLocalParallelReplicaStep *>(frame.node->step.get()))
        {
            read_from_local_parallel_replica_plan = true;

            auto local_plan = read_from_local->extractQueryPlan();
            local_plan->optimize(optimization_settings);

            auto * local_plan_node = frame.node;
            query_plan.replaceNodeWithPlan(local_plan_node, std::move(*local_plan));

            // after applying optimize() we still can have several expression in a row,
            // so merge them to make plan more concise
            if (optimization_settings.merge_expressions)
                tryMergeExpressions(local_plan_node, nodes, {});
        }

        stack.pop_back();
    }
    // local plan can contain redundant sorting
    if (read_from_local_parallel_replica_plan && optimization_settings.remove_redundant_sorting)
        tryRemoveRedundantSorting(&root);
    /// Optimize exchanges
    if (optimization_settings.make_distributed_plan && optimization_settings.distributed_plan_optimize_exchanges)
        optimizeExchanges(root);

    /// Vector search first pass optimization sets up everything for vector index usage.
    /// In the 2nd pass, we optimize further by attempting to do an "index-only scan".
    if (optimization_settings.try_use_vector_search && !extra_settings.vector_search_with_rescoring)
    {
        chassert(stack.empty());
        stack.push_back({.node = &root});
        while (!stack.empty())
        {
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                if (optimizeVectorSearchSecondPass(root, stack, nodes, extra_settings))
                    break;
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }

            stack.pop_back();
        }
        while (!stack.empty()) /// Vector search only for 1 substree with ORDER BY..LIMIT
            stack.pop_back();
    }

    /// projection optimizations can introduce additional reading step
    /// so, applying lazy materialization after it, since it's dependent on reading step
    if (optimization_settings.optimize_lazy_materialization)
    {
        chassert(stack.empty());
        stack.push_back({.node = &root});
        while (!stack.empty())
        {
            auto & frame = stack.back();

            if (frame.next_child == 0)
            {
                if (optimizeLazyMaterialization2(*frame.node, query_plan, nodes, optimization_settings, optimization_settings.max_limit_for_lazy_materialization))
                {
                    stack.pop_back();
                    continue;
                }
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }

            stack.pop_back();
        }
    }

    if (optimization_settings.force_use_projection && has_reading_from_mt && applied_projection_names.empty())
        throw Exception(
            ErrorCodes::PROJECTION_NOT_USED, "No projection is used when optimize_use_projections = 1 and force_optimize_projection = 1");

    if (!optimization_settings.force_projection_name.empty() && has_reading_from_mt
        && !applied_projection_names.contains(optimization_settings.force_projection_name))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Projection {} is specified in setting force_optimize_projection_name but not used",
            optimization_settings.force_projection_name);

    /// Trying to reuse sorting property for other steps.
    applyOrder(optimization_settings, root);

    if (optimization_settings.query_plan_join_shard_by_pk_ranges)
        optimizeJoinByShards(root);

    considerEnablingParallelReplicas(optimization_settings, root, query_plan);
}

void addStepsToBuildSets(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan & plan, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        /// NOTE: frame cannot be safely used after stack was modified.
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        addPlansForSets(optimization_settings, plan, *frame.node, nodes);

        stack.pop_back();
    }
}

}
}
