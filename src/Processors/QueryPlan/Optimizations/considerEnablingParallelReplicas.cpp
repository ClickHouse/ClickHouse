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
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromParallelReplicas.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
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
#include <tuple>
#include <unordered_set>

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

/// Collect the lazy reads inside one lazy-materialization branch, i.e. the branch of a
/// `JoinLazyColumnsStep` that `findReadingStep` does not descend into. The branch is a plan of its own
/// (`optimizeLazyMaterialization2` unites the main plan with a single-step lazy plan), so there is
/// normally exactly one lazy read and it sits at the branch root. Walk the branch anyway, so that a
/// later pass putting a step on top of it, or nesting another lazy materialization inside it, is seen
/// rather than silently missed.
void collectLazyReads(const QueryPlan::Node & branch_root, std::vector<LazilyReadFromMergeTree *> & lazy_reads)
{
    std::vector<const QueryPlan::Node *> to_visit{&branch_root};
    while (!to_visit.empty())
    {
        const auto * node = to_visit.back();
        to_visit.pop_back();

        /// The step is reached through a `shared_ptr`, so a const node still hands out a mutable step.
        if (auto * lazy = typeid_cast<LazilyReadFromMergeTree *>(node->step.get()))
            lazy_reads.push_back(lazy);

        for (const auto * child : node->children)
            to_visit.push_back(child);
    }
}

/// Find the read whose ranges parallel replicas would split between them, by descending from the node
/// whose output the replicas ship to the initiator.
///
/// `lazy_reading_step`, when passed, additionally reports the lazy half of that same read. Lazy
/// materialization splits one read in two: the `ReadFromMergeTree` returned here keeps the sorting
/// column, and a `LazilyReadFromMergeTree` under the sibling branch of a `JoinLazyColumnsStep` reads
/// the columns taken out of it. Both are executed by every replica - the plan the initiator ships is
/// the whole query, so each replica materializes its own rows lazily - so both belong in the same
/// statistics. Only the lazy reads met on this descent qualify: a lazy read on the join side we do not
/// descend into belongs to a different table, one that every replica reads in full rather than splits,
/// and the cost model divides `input_bytes` by the number of replicas.
ReadFromMergeTree * findReadingStep(
    const QueryPlan::Node & top_of_single_replica_plan, LazilyReadFromMergeTree ** lazy_reading_step = nullptr)
{
    if (lazy_reading_step)
        *lazy_reading_step = nullptr;

    std::vector<LazilyReadFromMergeTree *> lazy_reads;

    const auto * reading_step = &top_of_single_replica_plan;
    while (reading_step && !reading_step->children.empty())
    {
        // TODO(nickitat): support multiple read steps with parallel replicas
        const auto * lazy_joining = typeid_cast<const JoinLazyColumnsStep *>(reading_step->step.get());

        if (lazy_joining)
        {
            /// Unlike the `JoinStep` below, which side is which is not a decision here: this is not a SQL
            /// join, the step has neither a kind nor `swap_streams`, and its inputs are positional - input
            /// 0 is the main branch, input 1 the lazy one. `updatePipeline` hands the two pipelines to
            /// `LazyMaterializingTransform` in exactly that order, so the order is what makes the step
            /// work at all, not a convention this function relies on. Both places that build it
            /// (`optimizeLazyMaterialization2` and `optimizeLazyFinal`) unite the plans that way, and
            /// `unitePlans` rejects any other order because the headers would not line up.
            chassert(reading_step->children.size() == 2);
            collectLazyReads(*reading_step->children.back(), lazy_reads);
        }

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
    {
        if (lazy_reading_step)
        {
            // TODO(nickitat): support multiple read steps with parallel replicas
            if (lazy_reads.size() > 1)
                LOG_DEBUG(getLogger("optimizeTree"), "More than one lazy reading step, not collecting their statistics");
            else if (lazy_reads.size() == 1)
                *lazy_reading_step = lazy_reads.front();
        }
        return read_from_merge_tree;
    }

    LOG_DEBUG(
        getLogger("optimizeTree"),
        "Cannot find ReadFromMergeTree step in single-replica plan (found {}). Skipping optimization",
        reading_step->step->getName());
    return nullptr;
}

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

/// A read's identity for pairing: which table it reads, and which of that table's occurrences it is.
struct ReadIdentity
{
    const MergeTreeData * table;
    String table_expression_name;

    bool operator<(const ReadIdentity & other) const
    {
        return std::tie(table, table_expression_name) < std::tie(other.table, other.table_expression_name);
    }
};

/// Hand every read in the parallel replicas plan the analysis the single-node plan already produced for
/// the same read. Without this only the matched read gets an analysis and the rest scan everything - on
/// TPC-H q03, 1045 marks against 614.
///
/// An analysis carries the mark ranges selected for one read's predicates, so a pairing that lines the
/// two plans up wrongly does not merely misestimate - it reads the wrong rows. The reads are therefore
/// paired by the name the analyzer gave the table expression each one reads, which is stable across the
/// two plans and distinguishes two reads of one table.
///
/// Returns whether every read was paired. A read left unpaired keeps no analysis of its own either - the
/// replicas plan is built with `query_plan_optimize_primary_key` off - so it would read every mark, which
/// is the state this exists to avoid and which measured worse than not using replicas at all (TPC-H q22
/// at sf=100 was +96% against a single node). The caller declines the candidate instead.
bool transplantAnalysisToAllReads(QueryPlan::Node & single_node_root, QueryPlan::Node & replicas_root)
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
        return false;
    }

    /// Identify a read by the table expression it reads rather than by where it sits in the plan. The
    /// analyzer names every table expression (`__table1`, `__table2`, ...) while resolving the query, and
    /// both plans are built from the same query, so the names agree across them and tell two reads of one
    /// table apart - which the table alone cannot do, and which a self-join needs. Position cannot be
    /// trusted for this: the two plans are optimized differently and may order a join's sides differently.
    auto identify = [](const ReadFromMergeTree * read) -> std::optional<ReadIdentity>
    {
        const auto & table_expression = read->getQueryInfo().table_expression;
        if (!table_expression || table_expression->getAlias().empty())
            return {};
        return ReadIdentity{&read->getMergeTreeData(), table_expression->getAlias()};
    };

    std::map<ReadIdentity, ReadFromMergeTree *> single_node_by_identity;
    for (auto * read : single_node_reads)
    {
        auto identity = identify(read);
        if (!identity || !single_node_by_identity.emplace(*identity, read).second)
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Read of {} in the single-node plan has no name to pair it by, or shares one with another read; "
                "not transplanting index analysis",
                read->getStorageID().getNameForLogs());
            return false;
        }
    }

    std::vector<ReadFromMergeTree *> paired_single_node_reads(replicas_reads.size());
    std::unordered_set<const ReadFromMergeTree *> claimed_single_node_reads;
    for (size_t i = 0; i < replicas_reads.size(); ++i)
    {
        auto identity = identify(replicas_reads[i]);
        auto it = identity ? single_node_by_identity.find(*identity) : single_node_by_identity.end();
        if (it == single_node_by_identity.end())
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Read of {} in the replicas plan has no counterpart of the same name in the single-node plan; "
                "not transplanting index analysis",
                replicas_reads[i]->getStorageID().getNameForLogs());
            return false;
        }
        /// The names are unique on the single-node side because the map rejected a repeat, but two reads
        /// of the candidate can still look up the same one - and the plans have equally many reads, so a
        /// read claimed twice means another was not claimed at all, i.e. the plans do not read the same
        /// things. Pair one to one or not at all.
        if (!claimed_single_node_reads.insert(it->second).second)
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Two reads of {} in the replicas plan share one counterpart in the single-node plan; "
                "not transplanting index analysis",
                replicas_reads[i]->getStorageID().getNameForLogs());
            return false;
        }
        paired_single_node_reads[i] = it->second;
    }

    for (size_t i = 0; i < replicas_reads.size(); ++i)
    {
        /// Index analysis is lazy, so a read the single-node plan has not needed yet has no result to
        /// hand over. Produce it here, the same way the matched read step does: it is one analysis per
        /// read either way, and this way it is done once and shared instead of being repeated by the
        /// replicas plan.
        auto analyzed = paired_single_node_reads[i]->getAnalyzedResult();
        if (!analyzed)
            analyzed = paired_single_node_reads[i]->selectRangesToRead();

        /// A read that a projection answered selects that projection's parts and columns. The
        /// candidate is built with `optimize_projection` off, so its reads are of the base table and
        /// none of that applies to them. The pairing cannot tell the two apart: a projection read
        /// keeps the table and the table expression name of the base read it replaced. What keeps them
        /// apart today is the hash of the node the decision is matched on, taken bottom-up over its
        /// subtree: a read contributes only its name, its table and its `PREWHERE`, but the steps above
        /// a projection read serialize differently and the hash disagrees, so the optimization stops
        /// long before here. Decline rather than rest on that, for a read outside that subtree would
        /// reach this point.
        if (analyzed && analyzed->readFromProjection())
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Read of {} in the single-node plan is answered from a projection, which the plan for parallel "
                "replicas does not use; not transplanting index analysis",
                paired_single_node_reads[i]->getStorageID().getNameForLogs());
            return false;
        }

        if (analyzed)
        {
            replicas_reads[i]->setAnalyzedResult(analyzed);
            /// Hand over the conditions as well, not only the ranges they produced. The replicas plan is
            /// built with `query_plan_optimize_primary_key` off, so `applyFilters` never runs on its reads
            /// and a read that already has an analysis result never builds them later either.
            replicas_reads[i]->adoptFiltersFrom(*paired_single_node_reads[i]);
        }
    }

    return true;
}

/// Transplant the sets from the single-replica plan to the parallel-replicas plan once we decided to enable parallel replicas.
///
/// Both walks use `forEachSubquerySet` rather than a plain `traverseQueryPlan`, which follows only
/// `node->children`. A delayed set can also sit inside a set's own source plan (a nested `IN`) or -
/// on the parallel-replicas side - inside the local branch under `ReadFromLocalParallelReplicaStep`,
/// which is not a child node. A set missed here is not adopted from the single-replica plan and its
/// subquery runs a second time at execution, which is exactly what this transplant exists to avoid.
void moveSetsFromLocalPlanToReplicasPlan(const QueryPlan & single_replica_plan, const QueryPlan & parallel_replicas_plan)
{
    std::map<FutureSet::Hash, SetAndKeyPtr> sets_map;

    // Create a map: set_key -> set
    forEachSubquerySet(
        &single_replica_plan,
        [&](FutureSetFromSubquery & future_set)
        {
            if (auto set = future_set.detachSetAndKey())
                sets_map[future_set.getHash()] = std::move(set);
            return true;
        });

    // Now transplant the sets
    forEachSubquerySet(
        &parallel_replicas_plan,
        [&](FutureSetFromSubquery & future_set)
        {
            auto it = sets_map.find(future_set.getHash());
            if (it == sets_map.end())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Cannot find a matching set in the map of sets from single-replica plan");

            future_set.replaceSetAndKey(it->second);
            /// The set is built now, so this plan will not run and the sets nested in it will never be
            /// created. The single-replica plan has no counterparts for them either - its own nested
            /// sources are long gone by this point - so descending would only fail the lookup above.
            return false;
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

    /// Building the parallel-replicas plan below re-plans the query from scratch, which is expensive.
    /// Before paying for that, reject queries that read too little data for parallel replicas to be
    /// worth considering at all. The final check further down applies
    /// `automatic_parallel_replicas_min_bytes_per_replica` to the compressed bytes of the read that
    /// ends up being parallelized, which is not known until both plans are built and matched. Bound it
    /// here by the largest read in the plan: the parallelized read is one of them, so no read in the
    /// plan clearing the threshold means the parallelized one would not have cleared it either.
    ///
    /// The two byte counts are estimates of the same quantity but are not derived the same way: this
    /// one sums the compressed sizes the parts record for the columns read, while `input_bytes` is
    /// measured at runtime as in-memory bytes scaled by a sampled compression ratio. They agree
    /// closely for fixed-width columns and can differ by ~40% for columns of mostly-short strings,
    /// whose in-memory representation carries a per-row offset that the on-disk one does not. So a
    /// query just above the threshold can be rejected here - which is the intended trade: the gate
    /// exists to skip planning work, and the queries it can misjudge are the ones where parallel
    /// replicas barely pay off anyway.
    ///
    /// A read whose size cannot be estimated counts as large enough, so the gate never rejects on
    /// missing information. Mode 2 of `automatic_parallel_replicas_mode` only collects statistics and
    /// never switches to parallel replicas, and the threshold does not apply to it, so such queries
    /// are exempt and keep collecting statistics however little they read.
    const bool threshold_applies = optimization_settings.automatic_parallel_replicas_mode == 1
        && optimization_settings.automatic_parallel_replicas_min_bytes_per_replica != 0;
    if (threshold_applies)
    {
        const auto min_bytes_per_replica = optimization_settings.automatic_parallel_replicas_min_bytes_per_replica;
        const auto num_replicas = std::max<size_t>(optimization_settings.max_parallel_replicas, 1);

        /// The largest read measured so far. It is only read by the log message below, which is
        /// reached exactly when every read was measured, so it really is the largest read in the plan.
        size_t max_bytes_to_read = 0;
        bool found_read_worth_parallelizing = false;
        traverseQueryPlan(
            stack,
            root,
            [&](auto & frame_node)
            {
                /// One qualifying read is enough to keep the plan, and measuring a read runs index
                /// analysis, so stop measuring as soon as one is found.
                if (found_read_worth_parallelizing)
                    return;

                /// Only `ReadFromMergeTree`, deliberately. Lazy materialization splits one read in
                /// two - this step keeps the sorting column, and a `LazilyReadFromMergeTree` reads the
                /// columns taken out of it - and the lazy half is far the larger: its rows are spread
                /// over the whole table, so it touches almost every granule of them. It is still not
                /// what to size the plan by. `findReadingStep` descends into the first child of
                /// `JoinLazyColumnsStep`, so the read this loop measures is the one the optimization
                /// goes on to instrument and cost, and the only one it would parallelize. Sizing the
                /// plan by the lazy half instead would admit plans whose parallelizable read is tiny.
                const auto * reading = typeid_cast<const ReadFromMergeTree *>(frame_node.step.get());
                if (!reading)
                    return;

                /// A read whose size cannot be measured may be of any size, so it counts as
                /// qualifying: the gate must never reject a plan on missing information.
                const auto bytes_to_read = reading->estimateCompressedBytesToRead();
                if (!bytes_to_read || *bytes_to_read / num_replicas >= min_bytes_per_replica)
                {
                    found_read_worth_parallelizing = true;
                    return;
                }

                max_bytes_to_read = std::max(max_bytes_to_read, *bytes_to_read);
            });

        if (!found_read_worth_parallelizing)
        {
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "Not building the parallel replicas plan because the largest read in the plan gives at most {} bytes per replica, "
                "less than automatic_parallel_replicas_min_bytes_per_replica {}",
                max_bytes_to_read / num_replicas,
                min_bytes_per_replica);
            return;
        }
    }

    /// Hand the probe plan the sets this plan has already filled. It is built and optimized purely to
    /// decide whether replicas pay off, and optimizing it would otherwise re-run every `IN` subquery.
    ///
    /// Collecting here, before the analysis forced below, is early enough. The sets worth adopting are
    /// already filled: `optimizePrimaryKeyConditionAndLimit` runs earlier in this same pass and ends in
    /// `applyFilters`, where `buildIndexes` constructs the `KeyCondition` that calls
    /// `buildOrderedSetInplace` for every `IN` whose left argument maps to key columns. The
    /// `selectRangesToRead` below reuses those `indexes` (it builds them only `if (!indexes)`), so it
    /// adds no set that collecting later would catch.
    auto plan_with_parallel_replicas = optimization_settings.query_plan_with_parallel_replicas_builder(collectBuiltSets(query_plan));
    if (!plan_with_parallel_replicas)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot build a plan with parallel replicas. Skipping optimization");
        return;
    }

    const auto * final_node_in_replica_plan = findTopNodeOfReplicasPlan(plan_with_parallel_replicas->getRootNode());
    if (!final_node_in_replica_plan)
    {
        LOG_DEBUG(
            getLogger("optimizeTree"),
            "The plan built with parallel replicas contains no read from the other replicas. Skipping optimization");
        return;
    }
    LOG_DEBUG(getLogger("optimizeTree"), "Top node of replicas plan: {}", final_node_in_replica_plan->step->getName());

    const auto [corresponding_node_in_single_replica_plan, single_replica_plan_node_hash]
        = findCorrespondingNodeInSingleNodePlan(*final_node_in_replica_plan, *plan_with_parallel_replicas->getRootNode(), root);
    if (!corresponding_node_in_single_replica_plan)
        return;

    /// Now we need to identify the reading step that should be instrumented for statistics collection
    LazilyReadFromMergeTree * lazy_reading_step = nullptr;
    ReadFromMergeTree * source_reading_step = findReadingStep(*corresponding_node_in_single_replica_plan, &lazy_reading_step);
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
    /// A read served from a projection measures the projection's parts, not the table's, and the two
    /// plans need not agree on using it: `parallel_replicas_support_projection` is honoured only where
    /// a local plan is available, and the distributed path turns projections off outright (see
    /// `ClusterProxy::executeQuery`). The statistics key cannot tell the two apart either - a
    /// projection part belongs to the same storage and produces the same header - so a hash match
    /// between a projection-backed boundary here and a base-table boundary in the replicas plan would
    /// price one with the other's measurements, and `selected_rows` stays put so the drift check sees
    /// nothing. Skip, the way `force_use_projection` is skipped above: projection use is the one thing
    /// the parallel-replicas plan cannot be relied on to reproduce.
    if (analysis->readFromProjection())
    {
        LOG_DEBUG(getLogger("optimizeTree"), "The read is served from a projection. Skipping optimization");
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
            /// Ignoring the cap makes both sides of the comparison divide by the real thread counts.
            /// Otherwise a read too small to occupy `max_threads` clamps both sides to the same value,
            /// the comparison collapses to `0 > output_bytes / replicas`, and the query is decided here
            /// rather than by the cost model - which is the point of the cap outside of testing.
            const auto effective_max_reading_threads
                = (optimization_settings.automatic_parallel_replicas_ignore_thresholds
                   || !optimization_settings.min_bytes_per_task_for_reading)
                ? SIZE_MAX
                : stats->input_bytes / optimization_settings.min_bytes_per_task_for_reading + 1;
            const auto num_replicas = optimization_settings.max_parallel_replicas;
            /// Dividing `output_bytes` by `num_replicas` assumes the replicas partition the output
            /// between them, as they do for `Aggregating` or a plain `Sorting`. A boundary that keeps a
            /// bounded top-N per replica instead makes every replica ship the whole `output_bytes`:
            /// `LIMIT n`, `LIMIT -n` (the last `n` rows) and a `Sorting` carrying a limit. Dividing
            /// those underestimates the parallel-replicas plan by a factor of `num_replicas`.
            const auto * boundary_step = corresponding_node_in_single_replica_plan->step.get();
            const auto * boundary_sorting_step = typeid_cast<const SortingStep *>(boundary_step);
            const bool output_is_replicated = typeid_cast<const LimitStep *>(boundary_step)
                || typeid_cast<const NegativeLimitStep *>(boundary_step)
                || (boundary_sorting_step && boundary_sorting_step->getLimit() != 0);
            const size_t output_replicas_divisor = output_is_replicated ? 1 : num_replicas;
            const auto local_plan_cost_estimation = stats->input_bytes / std::min<size_t>(max_threads, effective_max_reading_threads);
            const auto replicas_plan_cost_estimation
                = (stats->input_bytes / std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads)) + stats->output_bytes / output_replicas_divisor;
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "The applied formula: {} / {} ? ({} / {} + {} / {}) ≡ {} ? {}",
                stats->input_bytes,
                std::min<size_t>(max_threads, effective_max_reading_threads),
                stats->input_bytes,
                std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads),
                stats->output_bytes,
                output_replicas_divisor,
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

                /// Every read of the candidate has to be given its analysis. One that is not would read
                /// every mark, so the candidate is worse than the plan it replaces; decline rather than run it.
                if (!transplantAnalysisToAllReads(*query_plan.getRootNode(), *plan_with_parallel_replicas->getRootNode()))
                    return;
                /// The candidate's reads have their filter actions only now, so the pass that tags a filter
                /// step for the query condition cache - which runs early in this same optimization and gives
                /// up when a read has none - saw nothing to tag, and the cache would never be populated by a
                /// query this optimization rewrote. Re-walk it, as the passes that rebuild filter steps do.
                if (optimization_settings.use_query_condition_cache)
                {
                    Stack qcc_stack;
                    qcc_stack.push_back({.node = plan_with_parallel_replicas->getRootNode()});
                    while (!qcc_stack.empty())
                    {
                        updateQueryConditionCache(qcc_stack, optimization_settings);

                        auto & qcc_frame = qcc_stack.back();
                        if (qcc_frame.next_child < qcc_frame.node->children.size())
                        {
                            auto * next_node = qcc_frame.node->children[qcc_frame.next_child];
                            ++qcc_frame.next_child;
                            qcc_stack.push_back({.node = next_node});
                            continue;
                        }
                        qcc_stack.pop_back();
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
                if (&local_replica_plan_reading_step->getMergeTreeData() != &source_reading_step->getMergeTreeData())
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Parallel replicas branch read is for table {} but the single-node plan reads {}",
                        local_replica_plan_reading_step->getStorageID().getNameForLogs(),
                        source_reading_step->getStorageID().getNameForLogs());
                }

                /// This read already carries the analysis, and it is this very one: the transplant above
                /// pairs it with the same single-node read that `findReadingStep` returns here, installs
                /// that read's analysis and filter state on it, and declines the candidate when any read
                /// cannot be paired - so reaching this point means it was. Assert rather than install it a
                /// second time. Firing here would mean the transplant's pairing and this descent disagree
                /// about which read the decision was matched on, which is worth knowing about: the reads
                /// would then be carrying ranges selected for another read's predicates.
                chassert(local_replica_plan_reading_step->getAnalyzedResult() == analysis);
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
        /// Share the updater with the lazy half of the same read so its bytes land in the same
        /// `input_bytes`. Without it the statistics describe only the sorting column, while the lazy
        /// read is the larger of the two by far, and the cost model prices the query on a fraction of
        /// what replicas read.
        if (lazy_reading_step)
            lazy_reading_step->setRuntimeDataflowStatisticsCacheUpdater(updater);
    }
}

}
}
