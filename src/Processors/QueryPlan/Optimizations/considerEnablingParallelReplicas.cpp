#include <Processors/QueryPlan/Optimizations/considerEnablingParallelReplicas.h>

#include <Core/Joins.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <map>
#include <optional>

using namespace DB::QueryPlanOptimizations;

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Is this the branch of the plan that reads from the other replicas? Both implementations of parallel
/// replicas are recognized: the query-based one (`ReadFromParallelRemoteReplicasStep`) and the plan-based
/// one (`ReadFromParallelReplicasStep`, enabled by `parallel_replicas_plan_based`).
bool isReadFromOtherReplicas(const IQueryPlanStep & step)
{
    return typeid_cast<const ReadFromParallelRemoteReplicasStep *>(&step)
        || typeid_cast<const ReadFromParallelReplicasStep *>(&step);
}

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
/// The plan-based implementation of parallel replicas (`parallel_replicas_plan_based`) builds the very
/// same shape, the only difference being that the branch reading from the other replicas is a
/// `ReadFromParallelReplicas` step, which ships a serialized plan fragment instead of a query.
///
QueryPlan::Node * findTopNodeOfReplicasPlan(QueryPlan::Node * plan_with_parallel_replicas_root)
{
    QueryPlan::Node * replicas_plan_top_node = nullptr;

    Stack stack;
    stack.push_back({.node = plan_with_parallel_replicas_root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Currently the approach is very simple: we look for Union step in the plan tree,
        /// and consider its children. The first child that is not a read from the other replicas
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
                if (!isReadFromOtherReplicas(*node->step))
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
std::pair<const QueryPlan::Node *, size_t> findCorrespondingNodeInSingleNodePlan(
    const QueryPlan::Node & final_node_in_replica_plan,
    QueryPlan::Node & parallel_replicas_plan_root,
    const std::unordered_map<const QueryPlan::Node *, UInt64> & single_replica_plan_hashes)
{
    auto pr_node_hashes = calculateHashTableCacheKeys(parallel_replicas_plan_root);
    if (auto it = pr_node_hashes.find(&final_node_in_replica_plan); it != pr_node_hashes.end())
    {
        for (const auto & [nopr_node, nopr_hash] : single_replica_plan_hashes)
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
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot find step with matching hash in single-node plan (looking for {})", it->second);
        return std::make_pair(nullptr, 0);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find replicas_plan_top_node in hash table");
    }
}

ReadFromMergeTree * findReadingStep(const QueryPlan::Node & top_of_single_replica_plan)
{
    const auto * reading_step = &top_of_single_replica_plan;
    while (reading_step && !reading_step->children.empty())
    {
        // TODO(nickitat): support multiple read steps with parallel replicas
        const auto * lazy_joining = typeid_cast<const JoinLazyColumnsStep *>(reading_step->step.get());

        // For a physical `JoinStep` (a plain `SELECT ... FROM a JOIN b` leaves it at/near the top of
        // the replicas plan), follow the parallelized side: child 0, or child 1 for `RIGHT`. This
        // mirrors the physical-slot selector used by `calculateHashTableCacheKeys` and
        // `ParallelReplicasLocalPlan`, so both resolve the same table as the parallelized input.
        if (const auto * join_step = typeid_cast<const JoinStep *>(reading_step->step.get());
            join_step && reading_step->children.size() == 2)
        {
            // `swap_streams` swaps the physical pipelines at execution without reordering the plan
            // children, so the kind-based side selection below would then descend into the wrong
            // child. In the analyzer path that AutoPR requires this is never set: joins are built as
            // `JoinStepLogical` and the logical->physical conversion applies any swap by reordering
            // the children and flipping the kind together (only the dead `optimizeJoinLegacy` path
            // sets `swap_streams`). Guard against it explicitly so that if a future change ever revives
            // it, AutoPR fails closed (skips) instead of instrumenting/parallelizing the wrong side.
            if (join_step->swap_streams)
                return nullptr;
            // Descending exactly one side is only a valid decomposition for join kinds that can be
            // evaluated by parallelizing one input while the other is read in full on every replica:
            // `INNER` (ALL), `LEFT`, and a leftmost `RIGHT`. It is NOT valid for `FULL` or
            // position-sensitive joins like `PASTE`, where a preserved-side row matched on another
            // replica would be emitted as unmatched here (or duplicated once per replica). We rely on
            // the upstream parallel-replicas eligibility checks for that: `findParallelReplicasQuery`
            // (`getSupportingParallelReplicasQueries` / `findTableForParallelReplicas`) admits only
            // those decomposable kinds and rejects `FULL`/`PASTE`/`CROSS`/etc., so for any other kind no
            // parallel-replicas plan is built and this function is never reached. The split below is
            // therefore safe by that invariant, not by a check here.
            const auto kind = join_step->getJoin()->getTableJoin().kind();
            reading_step = reading_step->children[isRight(kind) ? 1 : 0];
            continue;
        }

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

/// Whether `target` is somewhere in the subtree rooted at `subtree`.
bool subtreeContains(const QueryPlan::Node & subtree, const QueryPlan::Node & target)
{
    if (&subtree == &target)
        return true;

    for (const auto * child : subtree.children)
        if (subtreeContains(*child, target))
            return true;

    return false;
}

struct JoinAboveFragment
{
    const JoinStep * step = nullptr;
    UInt64 node_hash = 0;
    const QueryPlan::Node * build_side = nullptr;
};

/// The join whose probe side is the fragment the replicas would execute, if the plan has one. Shipping
/// that join's semi-join predicate into the fragment is what the decision below is about, so everything
/// this returns describes the *single-replica* plan: it is the plan the statistics were measured on, and
/// the one that keeps meaning the same thing whether or not we end up shipping.
///
/// Only a single join is handled. With more than one, the predicate the rewrite would inject is no longer
/// identified by "the join above the fragment", and the match rate of the wrong join would price it.
std::optional<JoinAboveFragment> findJoinAboveFragment(
    QueryPlan::Node & root,
    const QueryPlan::Node & fragment_top,
    const std::unordered_map<const QueryPlan::Node *, UInt64> & node_hashes)
{
    const QueryPlan::Node * join_node = nullptr;
    bool single_join = true;

    Stack stack;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & node)
        {
            if (!typeid_cast<const JoinStep *>(node.step.get()))
                return;
            single_join &= join_node == nullptr;
            join_node = &node;
        });

    if (!join_node || !single_join || join_node->children.size() != 2)
        return {};

    /// The predicate can only be shipped into the side the replicas execute, and the match rate the join
    /// counts is the one of its probe (left) side. Anything else - the fragment on the build side, or a
    /// join whose streams were swapped - is not the shape this prices.
    const auto * join_step = typeid_cast<const JoinStep *>(join_node->step.get());
    if (join_step->swap_streams || !subtreeContains(*join_node->children[0], fragment_top))
        return {};

    const auto hash_it = node_hashes.find(join_node);
    if (hash_it == node_hashes.end())
        return {};

    return JoinAboveFragment{.step = join_step, .node_hash = hash_it->second, .build_side = join_node->children[1]};
}

/// The number of rows the previous run's index analysis says a read scans, or nothing when the branch
/// does not end in a single MergeTree read.
std::optional<size_t> selectedRowsOf(const QueryPlan::Node & branch)
{
    ReadFromMergeTree * reading_step = findReadingStep(branch);
    if (!reading_step)
        return {};

    const auto analysis = reading_step->getAnalyzedResult() ? reading_step->getAnalyzedResult() : reading_step->selectRangesToRead();
    if (!analysis)
        return {};

    return analysis->selected_rows;
}

/// Whether the fragment already has the semi-join predicate applied to it in the single-replica plan, as a
/// join runtime filter. `optimize_move_to_prewhere` decides where it sits - pushed into the read's PREWHERE,
/// or a `FilterStep` above it - but either way it is inside the fragment and below its aggregation.
///
/// That makes it the same predicate this optimization ships, applied at the same point, and it means every
/// statistic the cost model just used was measured with it applied: the probe read fewer bytes and the
/// aggregation produced fewer groups than the replicas would without it. Those numbers describe the plan
/// that ships the predicate, not the one that does not, so shipping is what makes them true. It also makes
/// the join's own match rate useless here - the join counts what survived the filter, so it sees almost
/// nothing but matches, and would price the predicate it is already benefiting from at zero.
bool fragmentHasJoinRuntimeFilter(const QueryPlan::Node & fragment_top)
{
    const auto has_runtime_filter_atom = [](const ActionsDAG & dag, const String & filter_column_name)
    {
        const auto * predicate = dag.tryFindInOutputs(filter_column_name);
        if (!predicate)
            return false;
        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(predicate))
            if (atom->type == ActionsDAG::ActionType::FUNCTION && atom->function_base
                && atom->function_base->getName() == "__applyFilter")
                return true;
        return false;
    };

    std::vector<const QueryPlan::Node *> stack{&fragment_top};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get());
            filter && has_runtime_filter_atom(filter->getExpression(), filter->getFilterColumnName()))
            return true;

        if (const auto * read = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            if (const auto & prewhere = read->getPrewhereInfo();
                prewhere && has_runtime_filter_atom(prewhere->prewhere_actions, prewhere->prewhere_column_name))
                return true;

        for (const auto * child : node->children)
            stack.push_back(child);
    }

    return false;
}

/// Whether shipping the join's semi-join predicate into the replicas' fragment pays for itself.
///
/// Shipping trades one scan of the join's build side, done on the initiator to build the set, for the rows
/// each replica then does not have to read and aggregate. Both sides are counted in rows: the build side's
/// size in bytes is not measured anywhere, and rows are what the previous run measured on the other side.
///
/// The match rate is the join's, so it is a rate over the fragment's *output* rows - the groups an
/// aggregating fragment produces - while the predicate filters the fragment's *input* rows. The two agree
/// only when a key's group size does not depend on whether it matches; treat this as the estimate it is.
bool shouldShipJoinPredicate(
    const RuntimeDataflowStatistics & stats,
    const JoinAboveFragment & join,
    const QueryPlan::Node & fragment_top,
    size_t rows_to_read,
    size_t num_replicas)
{
    if (fragmentHasJoinRuntimeFilter(fragment_top))
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "The fragment is filtered by a join runtime filter on one node, which the replicas cannot be; "
            "shipping the join predicate so that they filter the same way the statistics were measured with");
        return true;
    }

    if (!stats.join_probe_rows)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "No join match rate was measured, not shipping the join predicate");
        return false;
    }

    if (stats.join_node_hash != join.node_hash)
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "The measured join match rate belongs to another join (hash {}, now {}), not shipping the join predicate",
            stats.join_node_hash,
            join.node_hash);
        return false;
    }

    const auto build_side_rows = selectedRowsOf(*join.build_side);
    if (!build_side_rows)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot size the join's build side, not shipping the join predicate");
        return false;
    }

    const double match_rate = static_cast<double>(stats.join_matched_probe_rows) / static_cast<double>(stats.join_probe_rows);
    const auto rows_saved_per_replica = static_cast<size_t>((1.0 - match_rate) * static_cast<double>(rows_to_read)) / num_replicas;

    LOG_DEBUG(
        getLogger("optimizeTree"),
        "Shipping the join predicate would save {} of {} rows per replica against a {} row scan to build the set "
        "(match rate {}/{})",
        rows_saved_per_replica,
        rows_to_read / num_replicas,
        *build_side_rows,
        stats.join_matched_probe_rows,
        stats.join_probe_rows);

    return rows_saved_per_replica > *build_side_rows;
}

/// Transplant the sets from the single-replica plan to the parallel-replicas plan once we decided to enable parallel replicas
void moveSetsFromLocalPlanToReplicasPlan(const QueryPlan & single_replica_plan, const QueryPlan & parallel_replicas_plan)
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
                    /// A set the parallel-replicas plan has and the single-replica plan does not is the
                    /// one this optimization injected itself: the shipped join predicate is a `GLOBAL IN`
                    /// that exists in no other plan. Leave it be - the merged plan is optimized further,
                    /// and `addStepsToBuildSets` expands what is still unbuilt.
                    if (auto it = sets_map.find(future_set->getHash()); it != sets_map.end())
                        future_set->replaceSetAndKey(it->second);
                }
            }
        });
}
}

namespace QueryPlanOptimizations
{

void considerEnablingParallelReplicas(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan & query_plan)
{
    if (!optimization_settings.automatic_parallel_replicas_mode || !optimization_settings.query_plan_with_parallel_replicas_builder)
        return;

    // Cannot guarantee projection usage with parallel replicas
    if (optimization_settings.force_use_projection)
        return;

    Stack stack;
    // Technically, it isn't required for all steps to support dataflow statistics collection,
    // but only for those that we will actually instrument (see `setRuntimeDataflowStatisticsCacheUpdater` calls below).
    // However, currently only relatively simple plans are supported (no UNIONs, etc.),
    // since such steps obviously don't support statistics collection, `supportsDataflowStatisticsCollection` is handy to check if the plan is simple enough.
    // `BuildRuntimeFilterStep` and `*CreatingSetsStep` don't collect statistics themselves but always appear below the instrumented top node,
    // so they are allowed to pass through the check.
    bool plan_is_simple_enough = true;
    String unsupported_steps;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            const bool step_is_supported = frame_node.step->supportsDataflowStatisticsCollection()
                || typeid_cast<const BuildRuntimeFilterStep *>(frame_node.step.get())
                || typeid_cast<const DelayedCreatingSetsStep *>(frame_node.step.get())
                || typeid_cast<const CreatingSetsStep *>(frame_node.step.get());
            if (!step_is_supported)
                unsupported_steps += (unsupported_steps.empty() ? "" : ", ") + frame_node.step->getUniqID();
            plan_is_simple_enough &= step_is_supported;
        });
    if (!plan_is_simple_enough)
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "Some steps in the plan don't support dataflow statistics collection. Skipping optimization. Unsupported steps: {}",
            unsupported_steps);
        return;
    }

    /// An explicitly set `parallel_replicas_ship_join_predicate` is a manual override: the single-node
    /// plan already carries the rewrite, so the plan with parallel replicas has to carry it too, and the
    /// cost-based choice below stays out of it.
    const auto manual_ship_join_predicate = optimization_settings.parallel_replicas_ship_join_predicate;

    auto plan_with_parallel_replicas = optimization_settings.query_plan_with_parallel_replicas_builder(manual_ship_join_predicate);
    if (!plan_with_parallel_replicas)
        return;

    const auto * final_node_in_replica_plan = findTopNodeOfReplicasPlan(plan_with_parallel_replicas->getRootNode());
    if (!final_node_in_replica_plan)
        return;
    LOG_DEBUG(getLogger("optimizeTree"), "Top node of replicas plan: {}", final_node_in_replica_plan->step->getName());

    const auto single_replica_plan_hashes = calculateHashTableCacheKeys(root);
    const auto [corresponding_node_in_single_replica_plan, single_replica_plan_node_hash]
        = findCorrespondingNodeInSingleNodePlan(
            *final_node_in_replica_plan, *plan_with_parallel_replicas->getRootNode(), single_replica_plan_hashes);
    if (!corresponding_node_in_single_replica_plan)
        return;

    /// Now we need to identify the reading step that should be instrumented for statistics collection
    ReadFromMergeTree * source_reading_step = findReadingStep(*corresponding_node_in_single_replica_plan);
    if (!source_reading_step)
        return;

    /// If the matched node is the reading step itself (e.g. a window function over a bare table scan:
    /// replicas would execute only the reading, everything above is computed on the initiator), we cannot
    /// estimate the number of bytes replicas would send to the initiator: the reading step records only
    /// input bytes (see `RuntimeDataflowStatisticsCacheUpdater::recordInputColumns`), while output bytes
    /// are recorded by the transforms of the steps above it. Proceeding would feed `output_bytes = 0` into
    /// the cost model, i.e. treat shipping the whole read result over the network as free, and could enable
    /// parallel replicas for plans that are cheaper to execute locally. Skip the optimization instead.
    if (corresponding_node_in_single_replica_plan->step.get() == source_reading_step)
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "The matched node is the reading step itself, cannot estimate the amount of data sent to the initiator. "
            "Skipping optimization");
        return;
    }

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
            // This value is an upper bound on the number of threads that can be used for reading (we simply don't have enough data to utilize more threads).
            // Since the Auto PR optimization is currently estimates only reading, it is better to use this value to avoid overestimating the benefits of PRs.
            const auto effective_max_reading_threads = optimization_settings.min_bytes_per_task_for_reading
                ? stats->input_bytes / optimization_settings.min_bytes_per_task_for_reading + 1
                : SIZE_MAX;
            const auto num_replicas = optimization_settings.max_parallel_replicas;
            const auto local_plan_cost_estimation = stats->input_bytes / std::min<size_t>(max_threads, effective_max_reading_threads);
            const auto replicas_plan_cost_estimation
                = (stats->input_bytes / std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads)) + stats->output_bytes / num_replicas;
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "The applied formula: {} / {} ? ({} / {} + {} / {}) ≡ {} ? {}",
                stats->input_bytes,
                std::min<size_t>(max_threads, effective_max_reading_threads),
                stats->input_bytes,
                std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads),
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

                /// The fragment goes to the replicas as text or as a serialized plan, so it cannot carry the
                /// join's runtime filter: the filter lives in the initiator's `RuntimeFilterLookup`. The
                /// predicate that filter approximates can be shipped instead, and rebuilding the plan is what
                /// puts it there - the rewrite happens in the analyzer, above everything this function sees.
                /// It is deliberately absent from the single-replica plan, which is the base the statistics
                /// above were measured against.
                bool shipped_join_predicate = false;
                if (!manual_ship_join_predicate)
                {
                    const auto join_above_fragment
                        = findJoinAboveFragment(root, *corresponding_node_in_single_replica_plan, single_replica_plan_hashes);

                    if (join_above_fragment
                        && shouldShipJoinPredicate(
                            *stats, *join_above_fragment, *corresponding_node_in_single_replica_plan, rows_to_read, num_replicas))
                    {
                        /// `globalIn`: the initiator evaluates the set once and broadcasts it. Plain `in` would
                        /// make every replica repeat the scan of the build side, which costs more than sending
                        /// the keys it distills down to.
                        auto plan_with_shipped_predicate = optimization_settings.query_plan_with_parallel_replicas_builder(2);
                        const auto * shipped_final_node
                            = plan_with_shipped_predicate ? findTopNodeOfReplicasPlan(plan_with_shipped_predicate->getRootNode()) : nullptr;

                        if (shipped_final_node)
                        {
                            plan_with_parallel_replicas = std::move(plan_with_shipped_predicate);
                            final_node_in_replica_plan = shipped_final_node;
                            shipped_join_predicate = true;
                            LOG_DEBUG(getLogger("optimizeTree"), "Shipping the join predicate into the replicas' fragment");
                        }
                        else
                        {
                            LOG_DEBUG(
                                getLogger("optimizeTree"),
                                "The plan with the join predicate shipped has no parallel replicas fragment, keeping the plain one");
                        }
                    }
                }

                ReadFromMergeTree * local_replica_plan_reading_step = findReadingStep(*final_node_in_replica_plan);
                if (!local_replica_plan_reading_step)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find ReadFromMergeTree step in local parallel replicas plan");

                /// Transplant the single-node index analysis onto the parallel-replicas branch read to honor
                /// parallel_replicas_index_analysis_only_on_coordinator (analyze once, reuse on the replica).
                /// For a plain table-on-top plan the freshly built branch read has no analysis yet. But the step
                /// may already carry an analysis: the planner runs index analysis on it when
                /// parallel_replicas_min_number_of_rows_per_replica > 0, and when a JOIN sits on top
                /// findReadingStep descends into one side whose read may already have been analyzed while
                /// planning the join (e.g. a top-level DISTINCT or a scalar subquery in the query). In that case
                /// keep its own analysis instead of overwriting it: it is the same parallelized table
                /// (findReadingStep runs the same descent on the hash-matched JOIN node in both plans, and the
                /// swap_streams case is already diverted to the throw above), so the existing result is
                /// equivalent. A read for a *different* table would mean the single-node and parallel-replicas
                /// plans diverged at the matched node - a broken invariant, so fail loudly rather than silently
                /// apply a mismatched analysis.
                ///
                /// Not when the join predicate was shipped: that analysis was made without the predicate, and
                /// reusing it would fix the mark ranges before the set exists - throwing away the granule
                /// pruning that is most of what shipping buys. Let the branch read analyze itself instead,
                /// once the set is there.
                if (shipped_join_predicate)
                {
                    LOG_DEBUG(
                        getLogger("optimizeTree"),
                        "Not reusing the single-node index analysis: the shipped predicate has to be analyzed with");
                }
                else if (local_replica_plan_reading_step->getAnalyzedResult() == nullptr)
                {
                    local_replica_plan_reading_step->setAnalyzedResult(analysis);
                }
                else if (&local_replica_plan_reading_step->getMergeTreeData() != &source_reading_step->getMergeTreeData())
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Parallel replicas branch read is analyzed for table {} but the single-node plan reads {}",
                        local_replica_plan_reading_step->getStorageID().getNameForLogs(),
                        source_reading_step->getStorageID().getNameForLogs());
                }
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

        /// Also measure the match rate of the join above the fragment, so the next run of this query can
        /// price shipping its semi-join predicate to the replicas. The join only counts it when the query
        /// was planned with analyze statistics on, which `enableJoinMatchRateCollectionIfNeeded` arranges
        /// for exactly the queries the rewrite can apply to.
        if (const auto join_above_fragment
            = findJoinAboveFragment(root, *corresponding_node_in_single_replica_plan, single_replica_plan_hashes))
            join_above_fragment->step->recordProbeMatchRateInto(updater, join_above_fragment->node_hash);
    }
}

}
}
