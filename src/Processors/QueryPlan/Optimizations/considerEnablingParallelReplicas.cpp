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

using namespace DB::QueryPlanOptimizations;

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

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
QueryPlan::Node * findTopNodeOfReplicasPlan(QueryPlan::Node * plan_with_parallel_replicas_root)
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
std::pair<const QueryPlan::Node *, size_t> findCorrespondingNodeInSingleNodePlan(
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

/// Transplant the sets from the single-replica plan to the parallel-replicas plan once we decided to enable parallel replicas
/// `allow_unmatched_sets` is for the plan that ships `IN` sets to the replicas: there the subquery has
/// been replaced by a temporary table, so the set the shipped plan builds is keyed by a different hash
/// and has no counterpart in the single-replica plan. Such a set builds itself from the temporary table,
/// which is exactly the work we wanted to move off the replicas, so leave it alone instead of failing.
/// For the probe plan the two plans must mirror each other, and an unmatched set stays a hard error.
/// Hand every read in the parallel replicas plan the analysis the single-node plan already produced for
/// the same read. The plans are built from the same query and differ only where the replicas step is
/// substituted, so their reads pair up in traversal order; the storage identity of each pair is checked
/// and the whole transplant is skipped if anything does not line up. Without this only the matched read
/// gets an analysis and the rest scan everything - on TPC-H q03, 1045 marks against 614.
std::vector<ReadFromMergeTree *> collectReadingSteps(QueryPlan::Node & root)
{
    Stack stack;
    std::vector<ReadFromMergeTree *> reading_steps;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            if (auto * reading_step = typeid_cast<ReadFromMergeTree *>(frame_node.step.get()))
                reading_steps.push_back(reading_step);
        });
    return reading_steps;
}

void transplantAnalysisToAllReads(QueryPlan::Node & single_node_root, QueryPlan::Node & replicas_root)
{
    auto single_node_reads = collectReadingSteps(single_node_root);
    auto replicas_reads = collectReadingSteps(replicas_root);

    if (single_node_reads.size() != replicas_reads.size())
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "Single-node plan has {} reads and the replicas plan {}; not transplanting index analysis",
            single_node_reads.size(),
            replicas_reads.size());
        return;
    }

    for (size_t i = 0; i < single_node_reads.size(); ++i)
    {
        if (&single_node_reads[i]->getMergeTreeData() != &replicas_reads[i]->getMergeTreeData())
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Read {} is {} in the single-node plan and {} in the replicas plan; not transplanting index analysis",
                i,
                single_node_reads[i]->getStorageID().getNameForLogs(),
                replicas_reads[i]->getStorageID().getNameForLogs());
            return;
        }
    }

    for (size_t i = 0; i < single_node_reads.size(); ++i)
    {
        /// Index analysis is lazy, so a read the single-node plan has not needed yet has no result to
        /// hand over. Produce it here, the same way the matched read step does: it is one analysis per
        /// read either way, and this way it is done once and shared instead of being repeated by the
        /// replicas plan.
        auto analyzed = single_node_reads[i]->getAnalyzedResult();
        if (!analyzed)
            analyzed = single_node_reads[i]->selectRangesToRead();
        if (analyzed)
            replicas_reads[i]->setAnalyzedResult(analyzed);
    }
}

/// Whether the query has any `IN (subquery)` set to ship at all. Without one the shipped plan is identical
/// to the probe, and building it a second time is pure waste - measured at ~0.5 ms of plan build plus
/// ~1.5 ms of optimization on a single-table query.
bool planHasSubquerySets(const QueryPlan & plan)
{
    Stack stack;
    bool has_sets = false;
    traverseQueryPlan(
        stack,
        *plan.getRootNode(),
        [&](auto & frame_node)
        {
            if (const auto * creating_sets_step = typeid_cast<const DelayedCreatingSetsStep *>(frame_node.step.get()))
                has_sets |= !creating_sets_step->getSets().empty();
        });
    return has_sets;
}

void moveSetsFromLocalPlanToReplicasPlan(
    const QueryPlan & single_replica_plan, const QueryPlan & parallel_replicas_plan, bool allow_unmatched_sets = false)
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
                    else if (allow_unmatched_sets)
                    {
                        LOG_DEBUG(
                            getLogger("optimizeTree"),
                            "Set {} has no counterpart in the single-replica plan; it is built from a shipped temporary table",
                            future_set->getHash().low64);
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

    /// Hand the probe plan the sets this plan has already filled. It is built and optimized purely to
    /// decide whether replicas pay off, and optimizing it would otherwise re-run every `IN` subquery.
    /// The probe plan is only used to decide whether replicas pay off, and the decision is made by
    /// matching its top node against the single-node plan by hash. Shipping `IN` sets replaces a
    /// subquery with a temporary table, which changes that hash and makes the match fail - so the probe
    /// is always built unshipped, and the shipped plan is built below only once replicas are chosen.
    auto built_sets = collectBuiltSets(query_plan);
    auto plan_with_parallel_replicas = optimization_settings.query_plan_with_parallel_replicas_builder(
        built_sets, /*ship_in_subqueries*/ false);
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

                transplantAnalysisToAllReads(*query_plan.getRootNode(), *plan_with_parallel_replicas->getRootNode());

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
                if (local_replica_plan_reading_step->getAnalyzedResult() == nullptr)
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
                /// Replicas are worth it. Only now is it worth building the variant that ships the `IN`
                /// sets: doing it for the probe would pay for materializing them even when the probe is
                /// thrown away, and would break the hash matching above.
                if (optimization_settings.parallel_replicas_ship_prepared_sets && planHasSubquerySets(query_plan))
                {
                    if (auto shipped_plan
                        = optimization_settings.query_plan_with_parallel_replicas_builder(
                            built_sets, /*ship_in_subqueries*/ true))
                    {
                        /// Redo on the shipped plan what was done to the probe: hand it the index analysis
                        /// the single-node plan already produced.
                        /// Hand the shipped plan the single-node index analysis. Unlike the probe, it must
                        /// be overwritten even when the shipped read already carries one: there the `IN` right
                        /// argument is a temporary table whose set is not built when the key condition is
                        /// evaluated, so nothing prunes and the analysis selects far more marks than the
                        /// single-node one the decision was based on (measured on TPC-H q03: 1045 marks
                        /// against 614, and three times the rows read).
                        transplantAnalysisToAllReads(*query_plan.getRootNode(), *shipped_plan->getRootNode());

                        const auto * shipped_top = findTopNodeOfReplicasPlan(shipped_plan->getRootNode());
                        ReadFromMergeTree * shipped_reading_step = shipped_top ? findReadingStep(*shipped_top) : nullptr;
                        if (shipped_reading_step
                            && &shipped_reading_step->getMergeTreeData() == &source_reading_step->getMergeTreeData())
                        {
                            shipped_reading_step->setAnalyzedResult(analysis);
                        }
                        else if (shipped_reading_step)
                        {
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Shipped parallel replicas plan reads {} but the single-node plan reads {}",
                                shipped_reading_step->getStorageID().getNameForLogs(),
                                source_reading_step->getStorageID().getNameForLogs());
                        }
                        moveSetsFromLocalPlanToReplicasPlan(query_plan, *shipped_plan, /*allow_unmatched_sets*/ true);
                        query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*shipped_plan));
                        return;
                    }
                    LOG_DEBUG(getLogger("optimizeTree"), "Could not build the plan that ships `IN` sets, using the probe plan");
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
    }
}

}
}
