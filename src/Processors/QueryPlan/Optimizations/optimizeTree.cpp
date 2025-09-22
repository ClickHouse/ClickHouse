#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/Exception.h>
#include "Interpreters/Context_fwd.h"

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <memory>
#include <stack>

namespace DB
{

namespace Setting
{
extern const SettingsBool enable_automatic_parallel_replicas;
extern const SettingsMaxThreads max_threads;
extern const SettingsNonZeroUInt64 max_parallel_replicas;
extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
}

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TOO_MANY_QUERY_PLAN_OPTIMIZATIONS;
extern const int PROJECTION_NOT_USED;
}

namespace QueryPlanOptimizations
{

void optimizeTreeFirstPass(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes)
{
    if (!optimization_settings.optimize_plan)
        return;

    const auto & optimizations = getOptimizations();

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
        optimization_settings.max_limit_for_vector_search_queries,
        optimization_settings.vector_search_with_rescoring,
        optimization_settings.vector_search_filter_strategy,
        optimization_settings.use_index_for_in_with_subqueries_max_values,
        optimization_settings.network_transfer_limits,
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
        for (const auto & optimization : optimizations)
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
                ++total_applied_optimizations;
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

QueryPlan::Node * findReplicasTopNode(QueryPlan::Node * plan_with_parallel_replicas_root)
{
    QueryPlan::Node * replicas_plan_top_node = nullptr;

    Stack stack;
    stack.push_back({.node = plan_with_parallel_replicas_root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (typeid_cast<UnionStep *>(frame.node->step.get()))
        {
            bool found_read_from_parallel_replicas = false;
            for (const auto & child : frame.node->children)
            {
                if (typeid_cast<const ReadFromParallelRemoteReplicasStep *>(child->step.get()))
                {
                    found_read_from_parallel_replicas = true;
                }
                else
                {
                    if (replicas_plan_top_node)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "");

                    replicas_plan_top_node = child;
                }
            }

            if (!found_read_from_parallel_replicas)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "");

            if (replicas_plan_top_node)
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

    if (!replicas_plan_top_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find top node for parallel replicas plan");
    return replicas_plan_top_node;
}

std::pair<const QueryPlan::Node *, size_t> findCorrespondingNodeInSingleNodePlan(
    const QueryPlan::Node & final_node_in_replica_plan,
    QueryPlan::Node & parallel_replicas_plan_root,
    QueryPlan::Node & single_replica_plan_root)
{
    auto pr_node_hashes = calculateHashTableCacheKeys(parallel_replicas_plan_root);
    if (auto it = pr_node_hashes.find(&final_node_in_replica_plan); it != pr_node_hashes.end())
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "final_node_in_replica_plan hash={}", it->second);
        auto nopr_node_hashes = calculateHashTableCacheKeys(single_replica_plan_root);

        for (const auto & [nopr_node, nopr_hash] : nopr_node_hashes)
        {
            if (nopr_hash == it->second)
            {
                LOG_DEBUG(&Poco::Logger::get("debug"), "Found matching node in original plan: {}", nopr_node->step->getName());
                return std::make_pair(nopr_node, nopr_hash);
            }
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find matching hash for replicas_plan_top_node");
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find hash for replicas_plan_top_node");
    }
}

void considerEnablingParallelReplicas(
    [[maybe_unused]] const QueryPlanOptimizationSettings & optimization_settings,
    [[maybe_unused]] QueryPlan::Node & root,
    [[maybe_unused]] QueryPlan::Nodes & nodes,
    [[maybe_unused]] QueryPlan & query_plan)
{
    if (!optimization_settings.query_plan_builder)
        return;

    if (optimization_settings.context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas])
        return;

    if (!optimization_settings.context->getSettingsRef()[Setting::enable_automatic_parallel_replicas])
        return;

    auto dump = [&](const QueryPlan & plan)
    {
        WriteBufferFromOwnString wb;
        plan.explainPlan(wb, ExplainPlanOptions{});
        LOG_DEBUG(&Poco::Logger::get("debug"), "\nwb.str()={}", wb.str());
    };

    auto plan_with_parallel_replicas = std::make_unique<QueryPlan>(optimization_settings.query_plan_builder());

    const auto * final_node_in_replica_plan = findReplicasTopNode(plan_with_parallel_replicas->getRootNode());
    chassert(final_node_in_replica_plan);
    LOG_DEBUG(&Poco::Logger::get("debug"), "replicas_plan_top_node->step->getName()={}", final_node_in_replica_plan->step->getName());

    [[maybe_unused]] const auto [corresponding_node_in_single_replica_plan, single_replica_plan_node_hash]
        = findCorrespondingNodeInSingleNodePlan(*final_node_in_replica_plan, *plan_with_parallel_replicas->getRootNode(), root);
    chassert(corresponding_node_in_single_replica_plan);

    {
        auto * reading_step = corresponding_node_in_single_replica_plan;
        while (reading_step && !reading_step->children.empty())
            // TODO(nickitat): Maybe we should consider all leafs?
            reading_step = reading_step->children.front();

        if (!typeid_cast<ReadFromMergeTree *>(reading_step->step.get()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The corresponding node in single node plan is not ReadFromMergeTree");

        reading_step->step->setDataflowCacheKey(single_replica_plan_node_hash);
        corresponding_node_in_single_replica_plan->step->setDataflowCacheKey(single_replica_plan_node_hash);
    }

    const auto & stats_cache = getRuntimeDataflowStatisticsCache();
    if (const auto stats = stats_cache.getStats(single_replica_plan_node_hash))
    {
        const auto max_threads = optimization_settings.context->getSettingsRef()[Setting::max_threads];
        const auto num_replicas = optimization_settings.context->getSettingsRef()[Setting::max_parallel_replicas];
        LOG_DEBUG(
            &Poco::Logger::get("debug"),
            "stats->input_bytes={}, stats->output_bytes={}, max_threads={}, num_replicas={}",
            stats->input_bytes,
            stats->output_bytes,
            max_threads.value,
            num_replicas.value);
        if (stats->input_bytes / max_threads >= (stats->input_bytes / (max_threads * num_replicas)) + stats->output_bytes / num_replicas)
        {
            dump(query_plan);
            query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(plan_with_parallel_replicas));
            dump(query_plan);
        }
    }
    else
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "The corresponding node in single node plan doesn't support runtime statistics");
    }
}


void optimizeTreeSecondPass(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan::Nodes & nodes, QueryPlan & query_plan)
{
    const size_t max_optimizations_to_apply = optimization_settings.max_optimizations_to_apply;
    std::unordered_set<String> applied_projection_names;
    bool has_reading_from_mt = false;

    Optimization::ExtraSettings extra_settings = {
        optimization_settings.max_limit_for_vector_search_queries,
        optimization_settings.vector_search_with_rescoring,
        optimization_settings.vector_search_filter_strategy,
        optimization_settings.use_index_for_in_with_subqueries_max_values,
        optimization_settings.network_transfer_limits,
    };

    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        optimizePrimaryKeyConditionAndLimit(stack);

        updateQueryConditionCache(stack, optimization_settings);

        /// NOTE: optimizePrewhere can modify the stack.
        /// Prewhere optimization relies on PK optimization (getConditionSelectivityEstimatorByPredicate)
        if (optimization_settings.optimize_prewhere)
            optimizePrewhere(stack, nodes);

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

    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            optimizeJoinLogical(frame_node, nodes, optimization_settings);
            optimizeJoinLegacy(frame_node, nodes, optimization_settings);
        },
        [&](auto & frame_node) { convertLogicalJoinToPhysical(frame_node, nodes, optimization_settings); });

    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            if (optimization_settings.read_in_order)
                optimizeReadInOrder(frame_node, nodes);

            if (optimization_settings.distinct_in_order)
                optimizeDistinctInOrder(frame_node, nodes);
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
                        optimization_settings.is_parallel_replicas_initiator_with_projection_support);
                    if (applied_projection)
                        applied_projection_names.insert(*applied_projection);
                }

                if (optimization_settings.aggregation_in_order)
                    optimizeAggregationInOrder(*frame.node, nodes);
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
                optimization_settings.is_parallel_replicas_initiator_with_projection_support))
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
            query_plan.replaceNodeWithPlan(local_plan_node, std::move(local_plan));

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
                if (optimizeLazyMaterialization(root, stack, nodes, optimization_settings.max_limit_for_lazy_materialization))
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

    considerEnablingParallelReplicas(optimization_settings, root, nodes, query_plan);
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
