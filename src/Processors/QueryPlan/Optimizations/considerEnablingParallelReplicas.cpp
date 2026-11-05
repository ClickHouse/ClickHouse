#include <Processors/QueryPlan/Optimizations/considerEnablingParallelReplicas.h>

#include <Common/ProfileEvents.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>
#include <Core/Joins.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
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
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <map>
#include <optional>
#include <set>
#include <tuple>
#include <unordered_set>

using namespace DB::QueryPlanOptimizations;

namespace ProfileEvents
{
extern const Event AutomaticParallelReplicasProbePlansBuilt;
extern const Event AutomaticParallelReplicasDecidedWithoutBuildingPlan;
extern const Event AutoParallelReplicasOptimizationMicroseconds;
extern const Event AutoParallelReplicasPlanBuildMicroseconds;
extern const Event AutoParallelReplicasSkippedDueToSettings;
extern const Event AutoParallelReplicasSkippedDueToQuery;
extern const Event AutoParallelReplicasPlansNotComparable;
extern const Event AutoParallelReplicasPlanWithoutParallelReplicas;
extern const Event AutoParallelReplicasNoStatistics;
extern const Event AutoParallelReplicasStatisticsDrifted;
extern const Event AutoParallelReplicasCostModelEvaluated;
extern const Event AutoParallelReplicasApplied;
extern const Event AutoParallelReplicasSkippedEarly;
extern const Event AutoParallelReplicasRejectedByThreshold;
extern const Event AutoParallelReplicasRejectedByDuplicatedReads;
extern const Event AutoParallelReplicasRejectedByCostModel;
extern const Event AutoParallelReplicasBytesPerReplica;
}

namespace DB
{

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

/// The planner leaves the read from the other replicas out where it turns parallel replicas off by itself,
/// e.g. for too few rows per replica or a correlated subquery.
bool planReadsFromOtherReplicas(QueryPlan::Node & root)
{
    bool found = false;
    Stack stack;
    traverseQueryPlan(stack, root, [&](const auto & frame_node) { found |= isReadFromOtherReplicas(*frame_node.step); });
    return found;
}

/// Steps that can sit between the `Union` and the node the two plans have in common without changing
/// what the node below computes. `FilterStep` is deliberately absent: the replicas ship what comes out
/// of it, so looking past one instruments a node carrying more rows than they would actually send. It
/// reports `supportsDataflowStatisticsCollection`, so it is matched on its own terms - and for a plain
/// `SELECT ... WHERE ...` it is the only node above the read, so peeling it would leave nothing to
/// instrument but the reading step itself.
///
/// The set and CTE bookkeeping steps pass their rows through by construction, in both their delayed and
/// their resolved form. The CTE pair is listed for completeness rather than for a shape that arrives here
/// today: the gate below refuses a plan that still carries a `DelayedMaterializingCTEsStep`, and the
/// comment there says why that refusal stays for now.
bool isPassthroughWrapper(const IQueryPlanStep & step)
{
    if (typeid_cast<const ExpressionStep *>(&step))
        return isPassthroughExpressionWithRenames(step);

    return typeid_cast<const DelayedCreatingSetsStep *>(&step)
        || typeid_cast<const CreatingSetsStep *>(&step)
        || typeid_cast<const DelayedMaterializingCTEsStep *>(&step)
        || typeid_cast<const MaterializingCTEsStep *>(&step);
}

/// Does this branch of the `Union` end in a read from the other replicas? Only the destination matters,
/// so every step with a single child is walked through regardless of what it computes.
bool branchReadsFromOtherReplicas(const QueryPlan::Node * node)
{
    while (!isReadFromOtherReplicas(*node->step) && node->children.size() == 1)
        node = node->children.front();

    return isReadFromOtherReplicas(*node->step);
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
                /// Classify this child first: is it the branch that reads from the other replicas? That
                /// branch is never instrumented, it only has to be recognized so that the `Union` is
                /// identified as the parallel-replicas pattern at all, so walk down to it through
                /// anything with a single child and do not ask what those steps do. Requiring them to be
                /// pass-through wrappers would make a branch we cannot see through look like a second
                /// node to instrument, and the whole query would be skipped as having "more than one local
                /// branch". `readingFromParallelReplicas` in
                /// `optimizeReadInOrder` walks the same chain for the same reason.
                if (branchReadsFromOtherReplicas(child))
                {
                    found_read_from_parallel_replicas = true;
                    continue;
                }

                /// This is the branch the initiator runs, and one of its nodes is what gets
                /// instrumented. Look through the wrappers that can sit between the `Union` and the node
                /// the two plans have in common. They stack in any order and any depth, so peel them in
                /// a loop rather than one of each kind - `Expression -> CreatingSets -> Expression` used
                /// to leave the search stranded on the second `Expression`.
                ///
                /// Stop above the reading step: it records only input bytes, so landing on it leaves
                /// no estimate of what the replicas would send and the optimization is skipped
                /// altogether. The last wrapper above it does record output bytes.
                auto * node = child;
                while (node->children.size() == 1 && isPassthroughWrapper(*node->step)
                       && !node->children.front()->children.empty())
                {
                    node = node->children.front();
                }

                if (replicas_plan_top_node)
                {
                    // TODO(nickitat): support multiple read steps with parallel replicas
                    LOG_TRACE(
                        getLogger("AutoParallelReplicas"),
                        "The plan built with parallel replicas has more than one local branch ({} and {}), only one is supported. "
                        "Skipping optimization",
                        replicas_plan_top_node->step->getName(),
                        node->step->getName());
                    return nullptr;
                }

                replicas_plan_top_node = node;
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

    if (!replicas_plan_top_node)
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "The plan built with parallel replicas contains no read from the other replicas. Skipping optimization");
    return replicas_plan_top_node;
}

/// `calculateHashTableCacheKeys` keys by plan node; `ExplainPlanOptions` keys by step, so that it
/// does not have to know about `QueryPlan::Node`. One node owns one step, so this is a rekeying.
std::unordered_map<const IQueryPlanStep *, UInt64> hashesByStep(const std::unordered_map<const QueryPlan::Node *, UInt64> & by_node)
{
    std::unordered_map<const IQueryPlanStep *, UInt64> by_step;
    by_step.reserve(by_node.size());
    for (const auto & [node, hash] : by_node)
        by_step.emplace(node->step.get(), hash);
    return by_step;
}

/// The plan as `EXPLAIN` prints it, with each step's hash alongside. The hashes are bottom-up, so
/// when two plans that should match do not, the deepest level at which the two dumps stop agreeing
/// is where they actually diverge - the one thing a failed match needs that a single line cannot
/// carry, which is why this sits at the test level and the summary stays at trace.
String explainWithHashes(const QueryPlan & plan, const std::unordered_map<const QueryPlan::Node *, UInt64> & hashes)
{
    const auto by_step = hashesByStep(hashes);
    ExplainPlanOptions options;
    options.step_hashes = &by_step;
    WriteBufferFromOwnString buffer;
    plan.explainPlan(buffer, options);
    return buffer.str();
}

using NodeHashes = std::unordered_map<const QueryPlan::Node *, UInt64>;

template <typename... Args>
UInt64 sipHash64Of(const Args &... args)
{
    SipHash hash;
    (hash.update(args), ...);
    return hash.get64();
}

/// Now when we found the top node of replicas plan, we need to find the corresponding node in the single node plan.
/// The working principle behind automatic parallel replicas is that we use statistics collected during execution of single-node plan
/// to estimate whether parallel replicas will be beneficial for the query or not. For that, we need to estimate how much data
/// replicas will send to the initiator. To do that, we found the node that will be at the top of replicas plan (e.g. Aggregating step in the example above),
/// and ask it collect statistics on the number of bytes it'd send to the initiator if we executed the query with parallel replicas.
std::pair<QueryPlan::Node *, size_t> findCorrespondingNodeInSingleNodePlan(
    std::optional<UInt64> wanted_hash, QueryPlan::Node & single_replica_plan_root, const NodeHashes & nopr_node_hashes)
{
    if (wanted_hash)
    {
        /// A step that contributes nothing to the key adopts its child's key (see
        /// `calculateHashTableCacheKeys`), so a whole chain of them answers to one hash. Find any member
        /// of that chain - the hash map's order is unspecified, so walk the plan for a stable answer -
        /// and then walk down to its bottom, which is the node that actually produces the numbers the
        /// chain shares. A wrapper above it measures something else: an `Aggregating` with a rename over
        /// it is such a chain, and the aggregation records the partial states the replicas ship where
        /// the rename above it already sees finalized values.
        QueryPlan::Node * matched_node = nullptr;
        Stack traversal_stack;
        const auto key_of = [&](const QueryPlan::Node & node) -> std::optional<UInt64>
        {
            if (auto hash_it = nopr_node_hashes.find(&node); hash_it != nopr_node_hashes.end())
                return hash_it->second;
            return {};
        };
        traverseQueryPlan(
            traversal_stack,
            single_replica_plan_root,
            [&](auto & frame_node)
            {
                if (!matched_node && key_of(frame_node) == *wanted_hash)
                    matched_node = &frame_node;
            });

        if (matched_node)
        {
            QueryPlan::Node * above = nullptr;
            while (matched_node->children.size() == 1 && key_of(*matched_node->children.front()) == *wanted_hash)
            {
                above = matched_node;
                matched_node = matched_node->children.front();
            }

            /// The reading step records input bytes only, so where the chain bottoms out in one, take
            /// the wrapper just above it instead - it does record output bytes. This mirrors
            /// `findTopNodeOfReplicasPlan` stopping above the read on the other side. A read with no
            /// wrapper above it is returned as it is, so the caller's fail-close for it stays reachable.
            if (matched_node->children.empty() && above)
                matched_node = above;
        }

        if (!matched_node)
        {
            /// Say what was looked for. Without the hash there is nothing to act on: this is by far the
            /// most common reason the optimization gives up.
            LOG_TRACE(getLogger("AutoParallelReplicas"), "Cannot find step with matching hash {} in single-node plan", *wanted_hash);
            return std::make_pair(nullptr, 0);
        }

        if (!matched_node->step->supportsDataflowStatisticsCollection())
        {
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Step ({}) doesn't support dataflow statistics collection. Skipping statistics collection",
                matched_node->step->getName());
            return std::make_pair(nullptr, 0);
        }

        LOG_TRACE(getLogger("AutoParallelReplicas"), "Found matching node in original plan: {}", matched_node->step->getName());
        return std::make_pair(matched_node, *wanted_hash);
    }
    else
    {
        return std::make_pair(nullptr, 0);
    }
}

/// The two reads the decision is about: the one whose ranges parallel replicas would split between them,
/// and the one standing in its place in the single-node plan - the plan whose steps get instrumented.
///
/// `lazy_to_instrument` is the lazy half of that same read. Lazy materialization splits one read in two:
/// the `ReadFromMergeTree` keeps the sorting column, and a `LazilyReadFromMergeTree` under the sibling
/// branch of a `JoinLazyColumnsStep` reads the columns taken out of it. Both are executed by every replica
/// - the plan the initiator ships is the whole query, so each replica materializes its own rows lazily -
/// so both belong in the same statistics. Only the lazy reads above the coordinated read qualify: a lazy
/// read on the join side that does not lead to it belongs to a different table, one that every replica
/// reads in full rather than splits, and the cost model divides `input_bytes` by the number of replicas.
struct CoordinatedReads
{
    ReadFromMergeTree * in_replicas_plan = nullptr;
    ReadFromMergeTree * to_instrument = nullptr;
    LazilyReadFromMergeTree * lazy_to_instrument = nullptr;
    bool several_lazy_reads = false;
    /// Cleared by anything that makes the answer untrustworthy: the two plans disagreeing about the shape,
    /// several reads being coordinated, or a step on the way that cannot be attributed to one of its
    /// inputs.
    bool usable = true;
};

/// Collect the lazy read inside one lazy-materialization branch, i.e. the branch of a
/// `JoinLazyColumnsStep` that does not lead to the coordinated read. The branch is a plan of its own
/// (`optimizeLazyMaterialization2` unites the main plan with a single-step lazy plan), so there is
/// normally exactly one lazy read and it sits at the branch root. Walk the branch anyway, so that a
/// later pass putting a step on top of it, or nesting another lazy materialization inside it, is seen
/// rather than silently missed.
void collectLazyReads(const QueryPlan::Node & branch_root, CoordinatedReads & reads)
{
    std::vector<const QueryPlan::Node *> to_visit{&branch_root};
    while (!to_visit.empty())
    {
        const auto * node = to_visit.back();
        to_visit.pop_back();

        /// The step is reached through a `shared_ptr`, so a const node still hands out a mutable step.
        if (auto * lazy = typeid_cast<LazilyReadFromMergeTree *>(node->step.get()))
        {
            // TODO(nickitat): support multiple read steps with parallel replicas
            if (reads.lazy_to_instrument || reads.several_lazy_reads)
            {
                /// Nothing here says which of them belongs to the coordinated read, so leave the lazy half
                /// out of the statistics rather than guess.
                reads.several_lazy_reads = true;
                reads.lazy_to_instrument = nullptr;
            }
            else
                reads.lazy_to_instrument = lazy;
        }

        for (const auto * child : node->children)
            to_visit.push_back(child);
    }
}


/// Which child of `node` stands in canonical position `index`. The hash the two plans were matched on
/// folds `A RIGHT JOIN B` and `B LEFT JOIN A` together - `calculateHashTableCacheKeys` swaps a `RIGHT`
/// join's children and remaps the kind to `LEFT` - so a hash match does not mean the two plans put the
/// same relation in the same plan child. Pair them in that same canonical order, or the walk below would
/// pair the two plans' relations the wrong way round on exactly the shapes the matcher accepts on
/// purpose. It matters most where the table cannot catch it: on a self-join both children read the same
/// table, so a wrongly paired child is instrumented rather than refused.
size_t canonicalChild(const QueryPlan::Node & node, size_t index)
{
    if (const auto * join_step = typeid_cast<const JoinStep *>(node.step.get());
        join_step && node.children.size() == 2 && isRight(join_step->getJoin()->getTableJoin().kind()))
    {
        /// Only ever the two children of this join: the caller walks indices of one plan and declines
        /// before that when the two plans' child counts differ, so a node with two children is asked
        /// about 0 and 1 only. Written so that a caller who breaks that stays in bounds - `1 - index`
        /// would wrap around, `size_t` being unsigned, and index far outside `children`.
        chassert(index < 2);
        return index == 0 ? 1 : 0;
    }
    return index;
}

/// A rename-only `Expression` gets its child's key in `calculateHashTableCacheKeys`, so the plans can match
/// with such a step present in one and absent in the other: when PREWHERE takes over a filter, the
/// single-node plan keeps one the parallel replicas local branch merges away.
const QueryPlan::Node & skipRenames(const QueryPlan::Node & node)
{
    const auto * current = &node;
    while (current->children.size() == 1 && isPassthroughExpressionWithRenames(*current->step))
        current = current->children.front();
    return *current;
}

/// Walk the two plans together and fill `reads`, returning whether the coordinated read is at or below
/// this pair of nodes. They can be walked together because they are identical below the node whose output
/// the replicas ship, which is what `findCorrespondingNodeInSingleNodePlan` establishes by comparing hashes
/// of the whole subtree. So nothing has to be searched for on the single-node side, which marks nothing:
/// the step standing where the replicas plan coordinates its read is the read to instrument.
///
/// Which table gets distributed is not this optimization's decision to make, and not one it should try to
/// reproduce either. It only has to price the read that *was* chosen, so it reads the choice off the plan
/// instead of deriving it. There is more than one rule to derive: query-based parallel replicas pin the
/// read to the query tree's left-most table expression (`findTableForParallelReplicas`), while
/// `parallel_replicas_plan_based` marks it in `applyParallelReplicas` through `collectReadsToDistribute`,
/// which descends a `JoinStepLogical` by `coordinatedJoinSide` over the post-swap children - so under that
/// setting a join swap moves which relation is distributed. Following the marker keeps the statistics in
/// step with either, and with whatever replaces them.
///
/// Deriving it is what went wrong. `children[isRight(kind) ? 1 : 0]`, which used to make the choice here,
/// cannot even see a swapped `INNER` join: `JoinStepLogical::swapInputs` reorders the inputs and flips
/// `Left` <-> `Right` but leaves `Inner` alone. Nor would the table do, because a self-join reads one table
/// twice and only one of the two occurrences is split between the replicas.
///
/// The nodes are taken as `const`, which still hands out mutable steps: `shared_ptr::get() const` returns
/// `T *`.
bool collectCoordinatedReads(const QueryPlan::Node & replicas_node_in, const QueryPlan::Node & single_node_in, CoordinatedReads & reads)
{
    const auto & replicas_node = skipRenames(replicas_node_in);
    const auto & single_node = skipRenames(single_node_in);

    if (auto * coordinated = typeid_cast<ReadFromMergeTree *>(replicas_node.step.get());
        coordinated && coordinated->isParallelReadingFromReplicas())
    {
        auto * to_instrument = typeid_cast<ReadFromMergeTree *>(single_node.step.get());
        if (!to_instrument || &to_instrument->getMergeTreeData() != &coordinated->getMergeTreeData())
        {
            reads.usable = false;
            return false;
        }

        reads.in_replicas_plan = coordinated;
        reads.to_instrument = to_instrument;
        return true;
    }

    if (replicas_node.children.size() != single_node.children.size())
    {
        reads.usable = false;
        return false;
    }

    bool found = false;
    for (size_t i = 0; i < replicas_node.children.size(); ++i)
    {
        const auto & replicas_child = *replicas_node.children[canonicalChild(replicas_node, i)];
        const auto & single_child = *single_node.children[canonicalChild(single_node, i)];
        if (!collectCoordinatedReads(replicas_child, single_child, reads))
            continue;
        if (found)
        {
            /// One coordinated read is what this optimization prices; see the `TODO(nickitat): support
            /// multiple read steps with parallel replicas` notes above.
            reads.usable = false;
            return false;
        }
        found = true;
    }

    if (!found)
        return false;

    /// This node is on the way to the read, so it is one of the steps the statistics have to survive.
    if (typeid_cast<const JoinLazyColumnsStep *>(single_node.step.get()))
    {
        /// Which side is which is not a decision here: this is not a SQL join, the step has neither a kind
        /// nor `swap_streams`, and its inputs are positional - input 0 is the main branch, input 1 the lazy
        /// one. `updatePipeline` hands the two pipelines to `LazyMaterializingTransform` in exactly that
        /// order, so the order is what makes the step work at all, not a convention this function relies
        /// on. Both places that build it (`optimizeLazyMaterialization2` and `optimizeLazyFinal`) unite the
        /// plans that way, and `unitePlans` rejects any other order because the headers would not line up.
        chassert(single_node.children.size() == 2);
        collectLazyReads(*single_node.children.back(), reads);
    }
    else if (single_node.children.size() > 1 && !typeid_cast<const JoinStep *>(single_node.step.get()))
    {
        /// A join is walked child by child like everything else. Any other step with several inputs is one
        /// the rest of the optimization has no account of, so fail closed.
        reads.usable = false;
    }

    return true;
}

QueryPlan::Node * findNodeOfStep(QueryPlan::Node & root, const IQueryPlanStep & step)
{
    QueryPlan::Node * found = nullptr;
    Stack stack;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            if (frame_node.step.get() == &step)
                found = &frame_node;
        });
    chassert(found);
    return found;
}

/// The read remembered as the coordinated one under `boundary`, found by its key. Two reads under the boundary
/// with the same key - a self-join reading one table the same way twice - cannot be told apart by it, so then
/// there is no answer and the replicas plan has to name the read.
ReadFromMergeTree * findCoordinatedReadByHash(QueryPlan::Node & boundary, UInt64 read_hash, const NodeHashes & nopr_node_hashes)
{
    ReadFromMergeTree * found = nullptr;
    size_t matches = 0;
    Stack stack;
    traverseQueryPlan(
        stack,
        boundary,
        [&](auto & frame_node)
        {
            auto * read = typeid_cast<ReadFromMergeTree *>(frame_node.step.get());
            if (!read)
                return;
            if (auto it = nopr_node_hashes.find(&frame_node); it != nopr_node_hashes.end() && it->second == read_hash)
            {
                found = read;
                ++matches;
            }
        });
    if (matches != 1)
    {
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "{} reads under the remembered boundary match the remembered coordinated read, building the replicas plan",
            matches);
        return nullptr;
    }
    return found;
}

/// Unlike `collectReadingSteps` it also returns `LazilyReadFromMergeTree`: a lazy half reads as much as the
/// step it belongs to, often more, and under parallel replicas every replica performs it.
std::vector<IQueryPlanStep *> collectReadSteps(QueryPlan::Node & root)
{
    Stack stack;
    std::vector<IQueryPlanStep *> read_steps;
    traverseQueryPlan(
        stack,
        root,
        [&](auto & frame_node)
        {
            auto * step = frame_node.step.get();
            if (typeid_cast<ReadFromMergeTree *>(step) || typeid_cast<LazilyReadFromMergeTree *>(step))
                read_steps.push_back(step);
        });
    return read_steps;
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
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "Single-node plan has {} reads and the replicas plan {}; not transplanting index analysis. Skipping optimization",
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
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Read of {} in the single-node plan has no name to pair it by, or shares one with another read; "
                "not transplanting index analysis. Skipping optimization",
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
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Read of {} in the replicas plan has no counterpart of the same name in the single-node plan; "
                "not transplanting index analysis. Skipping optimization",
                replicas_reads[i]->getStorageID().getNameForLogs());
            return false;
        }
        /// The names are unique on the single-node side because the map rejected a repeat, but two reads
        /// of the candidate can still look up the same one - and the plans have equally many reads, so a
        /// read claimed twice means another was not claimed at all, i.e. the plans do not read the same
        /// things. Pair one to one or not at all.
        if (!claimed_single_node_reads.insert(it->second).second)
        {
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Two reads of {} in the replicas plan share one counterpart in the single-node plan; "
                "not transplanting index analysis. Skipping optimization",
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
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Read of {} in the single-node plan is answered from a projection, which the plan for parallel "
                "replicas does not use; not transplanting index analysis. Skipping optimization",
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
///
/// Sets are paired by hash, and nothing guarantees that the two plans hash a set the same way: the
/// parallel-replicas plan is built from the query tree shipped to the replicas, and a set may be keyed
/// by something that differs between two builds of a plan (`convertJoinToIn` keys its set by a random
/// UUID). Returns false, leaving both plans untouched, when a set of the parallel-replicas plan has no
/// counterpart; the caller then keeps the single-replica plan.
bool moveSetsFromLocalPlanToReplicasPlan(const QueryPlan & single_replica_plan, const QueryPlan & parallel_replicas_plan)
{
    std::set<FutureSet::Hash> single_replica_hashes;
    forEachSubquerySet(
        &single_replica_plan,
        [&](FutureSetFromSubquery & future_set)
        {
            if (future_set.getSetAndKey())
                single_replica_hashes.insert(future_set.getHash());
            return true;
        });

    bool all_sets_paired = true;
    forEachSubquerySet(
        &parallel_replicas_plan,
        [&](FutureSetFromSubquery & future_set)
        {
            if (!single_replica_hashes.contains(future_set.getHash()))
                all_sets_paired = false;
            /// Mirrors the walk below, which does not descend into a set it pairs.
            return false;
        });

    if (!all_sets_paired)
        return false;

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
            future_set.replaceSetAndKey(sets_map.at(future_set.getHash()));
            /// The set is built now, so this plan will not run and the sets nested in it will never be
            /// created. The single-replica plan has no counterparts for them either - its own nested
            /// sources are long gone by this point - so descending would only fail the lookup above.
            return false;
        });

    return true;
}
}

namespace QueryPlanOptimizations
{

static bool decideAtBoundary(
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan & query_plan,
    QueryPlan::Node * single_replica_plan_boundary,
    size_t single_replica_plan_boundary_hash,
    ReadFromMergeTree * source_reading_step,
    const CoordinatedReads * reads,
    const QueryPlanPtr & plan_with_parallel_replicas,
    const QueryPlan::Node * replicas_plan_boundary);

void considerEnablingParallelReplicas(
    const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root, QueryPlan & query_plan)
{
    if (!optimization_settings.automatic_parallel_replicas_mode || !optimization_settings.query_plan_with_parallel_replicas_builder)
        return;

    /// Everything from here on is paid whether or not the plan with parallel replicas is adopted,
    /// and these counters are the only way to see that cost from `system.query_log`. Building the plan
    /// is counted apart from the rest, so the two do not overlap.
    Stopwatch watch;
    UInt64 plan_build_microseconds = 0;
    SCOPE_EXIT({
        ProfileEvents::increment(
            ProfileEvents::AutoParallelReplicasOptimizationMicroseconds, watch.elapsedMicroseconds() - plan_build_microseconds);
    });

    /// Cannot guarantee projection usage with parallel replicas. `buildQueryPlanForAutomaticParallelReplicas`
    /// builds the candidate with projections off entirely - it clears `optimize_projection`,
    /// `force_use_projection` and `force_projection_name` - while `optimizeTree` has already enforced
    /// both forcing settings against the single-node plan by the time this runs. Adopting the candidate
    /// would therefore answer a query that demanded a projection with a plan that uses none.
    ///
    /// Today node matching would reject such a candidate anyway, because a plan reading a projection
    /// hashes differently from one reading the table. That is an accident of how the hashes are built,
    /// not a guarantee, so state the requirement here instead of relying on it.
    if (optimization_settings.force_use_projection || !optimization_settings.force_projection_name.empty())
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedDueToSettings);
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "force_optimize_projection or force_optimize_projection_name is set, cannot guarantee projection usage. "
            "Skipping optimization");
        return;
    }

    /// A plan that builds a set is rooted in the `CreatingSetStep` that fills it, and that root is what
    /// gives the plan the empty header `addPlansForSets` attaches to a `CreatingSetsStep`. Switching to
    /// the parallel-replicas candidate replaces the root (see `replaceNodeWithPlan` below) and the
    /// candidate is built without one, so the set plan would come back carrying the subquery's own
    /// columns and `CreatingSetsStep` would reject it with "Creating set input must have empty header".
    /// Leave these plans alone until the candidate can carry the set-building root over.
    ///
    /// Only the root matters. A `CreatingSetStep` further down belongs to a set this plan consumes
    /// rather than builds, and the steps above it are replaced without disturbing it.
    ///
    /// Refusing here also saves the probe plan, which is what makes the refusal observable:
    /// `05218_autopr_set_building_plan_is_left_alone` counts the rounds of index analysis a `GLOBAL IN`
    /// runs, and considering the set-building plan adds one.
    if (typeid_cast<const CreatingSetStep *>(root.step.get()))
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedDueToQuery);
        LOG_TRACE(getLogger("AutoParallelReplicas"), "The plan builds a set, its root must be preserved. Skipping optimization");
        return;
    }

    Stack stack;
    // Technically, it isn't required for all steps to support dataflow statistics collection,
    // but only for those that we will actually instrument (see `setRuntimeDataflowStatisticsCacheUpdater` calls below).
    // However, currently only relatively simple plans are supported (no UNIONs, etc.),
    // since such steps obviously don't support statistics collection, `supportsDataflowStatisticsCollection` is handy to check if the plan is simple enough.
    // `BuildRuntimeFilterStep` and `*CreatingSetsStep` don't collect statistics themselves but always appear below the instrumented top node,
    // so they are allowed to pass through the check.
    //
    // `DelayedMaterializingCTEsStep` is transparent in the same way and is deliberately NOT allowed
    // through, which is what keeps a query with `enable_materialized_cte` out of the optimization:
    // `resolveMaterializingCTEs` runs after this pass (`QueryPlan::optimize`), so the placeholder is still
    // in the plan when the gate looks at it. Letting it through buys nothing yet - the boundary is then
    // found, but it matches nothing in the single-node plan, and the parallel-replicas build of such a
    // query carries no remote read at all - so the query would pay for a probe plan, index analysis
    // included, and still not be parallelized. It belongs here once matching handles a materialized CTE;
    // `isPassthroughWrapper` above is ready for it.
    //
    // `MaterializingCTEStep` - the singular one, which caps a CTE's own plan - is a different matter and
    // must not be added without extending the set-building refusal below to that root too. Such a plan is
    // optimized like any other (`DelayedMaterializingCTEsStep::optimizePlans`) and switching it would
    // replace the very step that fills the CTE, exactly as for `CreatingSetStep`.
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
                || typeid_cast<const CreatingSetsStep *>(frame_node.step.get())
                || typeid_cast<const CreatingSetStep *>(frame_node.step.get());
            if (!step_is_supported)
                unsupported_steps += (unsupported_steps.empty() ? "" : ", ") + frame_node.step->getUniqID();
            plan_is_simple_enough &= step_is_supported;
        });
    if (!plan_is_simple_enough)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedDueToQuery);
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
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
                /// what to size the plan by. The walk above goes into the first child of
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
            /// These queries never reach the cost model, so this is their only chance to contribute
            /// to the distribution, and this gate is the one that turns most queries away. Only the
            /// rejected side can be recorded here: when a read does qualify the loop above stops
            /// early, deliberately, because measuring one runs index analysis - so on that path
            /// `max_bytes_to_read` is not the largest read and the cost model records the better
            /// number anyway.
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedEarly);
            ProfileEvents::increment(
                ProfileEvents::AutoParallelReplicasBytesPerReplica, max_bytes_to_read / num_replicas);
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Not building the parallel replicas plan because the largest read in the plan gives at most {} bytes per replica, "
                "less than automatic_parallel_replicas_min_bytes_per_replica {}",
                max_bytes_to_read / num_replicas,
                min_bytes_per_replica);
            return;
        }
    }

    /// The boundary is where the replicas' share of the work ends: `replicas_plan_boundary` in the replicas plan,
    /// `single_replica_plan_boundary` its counterpart in this, single-replica, plan. An earlier execution that
    /// matched them left the latter's key under the single-replica plan's root, so no replicas plan has to be
    /// built to find it again. Mode 2 recollects, so it always matches against a fresh replicas plan, and with
    /// `automatic_parallel_replicas_remember_boundary` off every query does.
    const auto nopr_node_hashes = calculateHashTableCacheKeys(root);
    auto & stats_cache = getRuntimeDataflowStatisticsCache();
    /// Where the replicas' half ends also depends on settings that leave the single-replica plan as it is, so the
    /// key carries them beside the root's hash.
    const auto root_key = sipHash64Of(nopr_node_hashes.at(&root), optimization_settings.parallel_replicas_boundary_settings_hash);
    QueryPlan::Node * single_replica_plan_boundary = nullptr;
    size_t single_replica_plan_boundary_hash = 0;
    ReadFromMergeTree * remembered_coordinated_read = nullptr;
    const bool remember_boundary = optimization_settings.automatic_parallel_replicas_remember_boundary;
    if (remember_boundary && optimization_settings.automatic_parallel_replicas_mode != 2)
    {
        if (const auto remembered = stats_cache.getBoundary(root_key))
        {
            std::tie(single_replica_plan_boundary, single_replica_plan_boundary_hash)
                = findCorrespondingNodeInSingleNodePlan(remembered->boundary_hash, root, nopr_node_hashes);
            if (single_replica_plan_boundary)
                remembered_coordinated_read
                    = findCoordinatedReadByHash(*single_replica_plan_boundary, remembered->coordinated_read_hash, nopr_node_hashes);
        }
    }

    /// A boundary remembered from an earlier execution lets the statistics decline without building the replicas
    /// plan. Everything else - adopting, collecting, or no remembered boundary - needs that plan: the cached key
    /// cannot see everything that decides where the replicas' half ends, so the plan names the boundary and the
    /// decision is taken again there.
    if (remembered_coordinated_read
        && !decideAtBoundary(
            optimization_settings,
            query_plan,
            single_replica_plan_boundary,
            single_replica_plan_boundary_hash,
            remembered_coordinated_read,
            /*reads=*/nullptr,
            /*plan_with_parallel_replicas=*/nullptr,
            /*replicas_plan_boundary=*/nullptr))
    {
        /// Counted here rather than at each decline in `decideAtBoundary`, so that none of them is missed.
        ProfileEvents::increment(ProfileEvents::AutomaticParallelReplicasDecidedWithoutBuildingPlan);
        return;
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
    auto built_sets = collectBuiltSets(query_plan);
    Stopwatch plan_build_watch;
    auto build_result = optimization_settings.query_plan_with_parallel_replicas_builder(built_sets, getLogger("AutoParallelReplicas"));

    /// The builder checks what it can before it builds anything, and logs the specific reason when it
    /// stops there. Those are skips, like the ones above, not plans that could not be compared.
    switch (build_result.skip_reason)
    {
        case PlanWithParallelReplicas::SkipReason::Settings:
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedDueToSettings);
            return;
        case PlanWithParallelReplicas::SkipReason::QueryShape:
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasSkippedDueToQuery);
            return;
        case PlanWithParallelReplicas::SkipReason::None:
            break;
    }
    chassert(build_result.plan);
    ProfileEvents::increment(ProfileEvents::AutomaticParallelReplicasProbePlansBuilt);
    /// Only a plan that was built is charged as building one; a skip stays in the rest of the optimization.
    plan_build_microseconds = plan_build_watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlanBuildMicroseconds, plan_build_microseconds);
    auto plan_with_parallel_replicas = std::move(build_result.plan);

    auto * const replicas_plan_boundary = findTopNodeOfReplicasPlan(plan_with_parallel_replicas->getRootNode());
    if (!replicas_plan_boundary)
    {
        /// `findTopNodeOfReplicasPlan` has logged why. Without any read from the other replicas, the planner has
        /// declined parallel replicas for this query, which is not a limitation of this optimization.
        if (planReadsFromOtherReplicas(*plan_with_parallel_replicas->getRootNode()))
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        else
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlanWithoutParallelReplicas);
        return;
    }
    LOG_TRACE(getLogger("AutoParallelReplicas"), "Top node of replicas plan: {}", replicas_plan_boundary->step->getName());

    const auto pr_node_hashes = calculateHashTableCacheKeys(*plan_with_parallel_replicas->getRootNode());
    std::tie(single_replica_plan_boundary, single_replica_plan_boundary_hash)
        = findCorrespondingNodeInSingleNodePlan(pr_node_hashes.at(replicas_plan_boundary), root, nopr_node_hashes);
    if (!single_replica_plan_boundary)
    {
        /// `findCorrespondingNodeInSingleNodePlan` has logged why. Both plans in full, each step tagged with
        /// its hash: `LOG_IMPL` leaves before it touches its arguments when the level is off, so neither plan
        /// is walked unless someone asked for this.
        LOG_TEST(
            getLogger("AutoParallelReplicas"),
            "No usable match for hash {}. Plan with parallel replicas:\n{}\nSingle-node plan:\n{}",
            pr_node_hashes.at(replicas_plan_boundary),
            explainWithHashes(*plan_with_parallel_replicas, pr_node_hashes),
            explainWithHashes(query_plan, nopr_node_hashes));
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        return;
    }

    /// Now we need to identify the reading step that should be instrumented for statistics collection.
    /// It has to be the read parallel replicas will actually coordinate, because the cost model divides its
    /// `input_bytes` by the replica count, and only the read whose mark ranges the replicas split between
    /// them earns that division - otherwise the model prices a table nobody splits. Only the replicas plan
    /// knows which read that is, so walk the two plans together and take the step standing in its place.
    CoordinatedReads reads;
    const bool found_coordinated_read = collectCoordinatedReads(*replicas_plan_boundary, *single_replica_plan_boundary, reads);
    if (!reads.usable)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "The single-node plan and the replicas plan do not agree about the read coordinated between the "
            "replicas, or more than one is. Skipping optimization");
        return;
    }
    if (!found_coordinated_read)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "No read below the node whose output the replicas ship is coordinated between them, so nothing "
            "would be parallelized. Skipping optimization");
        return;
    }

    if (reads.several_lazy_reads)
        LOG_TRACE(getLogger("AutoParallelReplicas"), "More than one lazy reading step, not collecting their statistics");

    /// Remember the coordinated read along with the boundary: a later execution that declines at the boundary
    /// has no replicas plan to walk alongside.
    if (remember_boundary)
        stats_cache.setBoundary(
            root_key,
            {.boundary_hash = single_replica_plan_boundary_hash,
             .coordinated_read_hash = nopr_node_hashes.at(findNodeOfStep(*single_replica_plan_boundary, *reads.to_instrument))});

    /// The replicas plan exists now, so the decision cannot ask for it again.
    [[maybe_unused]] const bool needs_replicas_plan = decideAtBoundary(
        optimization_settings,
        query_plan,
        single_replica_plan_boundary,
        single_replica_plan_boundary_hash,
        reads.to_instrument,
        &reads,
        plan_with_parallel_replicas,
        replicas_plan_boundary);
    chassert(!needs_replicas_plan);
}

/// Prices the query at `single_replica_plan_boundary` and then parallelizes it, leaves it alone, or
/// instruments it to collect statistics. `source_reading_step` is the read parallel replicas coordinate, as
/// it stands in this plan. Returns true only when the verdict is to parallelize or to collect but there is no
/// replicas plan yet - the boundary came from the cache - so the caller is to build the replicas plan and call
/// again with the boundary it matches. `reads` comes with the replicas plan, and is null when that is.
static bool decideAtBoundary(
    const QueryPlanOptimizationSettings & optimization_settings,
    QueryPlan & query_plan,
    QueryPlan::Node * const single_replica_plan_boundary,
    size_t single_replica_plan_boundary_hash,
    ReadFromMergeTree * const source_reading_step,
    const CoordinatedReads * const reads,
    const QueryPlanPtr & plan_with_parallel_replicas,
    const QueryPlan::Node * const replicas_plan_boundary)
{
    /// If the matched node is the reading step itself (e.g. a window function over a bare table scan:
    /// replicas would execute only the reading, everything above is computed on the initiator), we cannot
    /// estimate the number of bytes replicas would send to the initiator: the reading step records only
    /// input bytes (see `RuntimeDataflowStatisticsCacheUpdater::recordInputColumns`), while output bytes
    /// are recorded by the transforms of the steps above it. Proceeding would feed `output_bytes = 0` into
    /// the cost model, i.e. treat shipping the whole read result over the network as free, and could enable
    /// parallel replicas for plans that are cheaper to execute locally. Skip the optimization instead.
    if (single_replica_plan_boundary->step.get() == source_reading_step)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(
            getLogger("AutoParallelReplicas"),
            "The matched node is the reading step itself, cannot estimate the amount of data sent to the initiator. "
            "Skipping optimization");
        return false;
    }

    const auto analysis
        = source_reading_step->getAnalyzedResult() ? source_reading_step->getAnalyzedResult() : source_reading_step->selectRangesToRead();
    if (!analysis)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(getLogger("AutoParallelReplicas"), "Cannot get index analysis result from MergeTree table. Skipping optimization");
        return false;
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
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(getLogger("AutoParallelReplicas"), "The read is served from a projection. Skipping optimization");
        return false;
    }
    const auto rows_to_read = analysis->selected_rows;
    if (!rows_to_read)
    {
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
        LOG_TRACE(getLogger("AutoParallelReplicas"), "Index analysis result doesn't contain selected rows. Skipping optimization");
        return false;
    }

    bool table_data_drifted_significantly = true;

    const auto & stats_cache = getRuntimeDataflowStatisticsCache();
    if (const auto stats = stats_cache.getStats(single_replica_plan_boundary_hash))
    {
        bool apply_plan_with_parallel_replicas = optimization_settings.automatic_parallel_replicas_mode != 2;
        if (std::max<size_t>(stats->total_rows_to_read, rows_to_read) > std::min<size_t>(stats->total_rows_to_read, rows_to_read) * 2)
        {
            /// Statistics are recollected only with the replicas plan; the call with it counts the event.
            if (!plan_with_parallel_replicas)
                return true;

            /// With the hash: without it there is no way to tell genuine drift on one plan from two
            /// different plans sharing a cache entry, and the two look identical in the log.
            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasStatisticsDrifted);
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "Significant difference in total rows from storage detected for hash {} (previously {}, now {}). Recollecting statistics",
                single_replica_plan_boundary_hash,
                stats->total_rows_to_read,
                rows_to_read);
            apply_plan_with_parallel_replicas = false;
        }
        else
        {
            table_data_drifted_significantly = false;
            /// No counter here when mode 2 leaves `apply_plan_with_parallel_replicas` false. Every
            /// other way of reaching this point ends in one of the events above, so in modes 0 and 1
            /// the attempts balance against the outcomes. Mode 2 only ever collects statistics and
            /// never decides, so it sits outside that accounting by construction, and it is not a
            /// mode production runs in - a dedicated event for it would only be noise there.
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
            /// Dividing `output_bytes` by `num_replicas` assumes the replicas partition the output
            /// between them, as they do for `Aggregating` or a plain `Sorting`. A boundary that keeps a
            /// bounded top-N per replica instead makes every replica ship the whole `output_bytes`:
            /// `LIMIT n`, `LIMIT -n` (the last `n` rows) and a `Sorting` carrying a limit. Dividing
            /// those underestimates the parallel-replicas plan by a factor of `num_replicas`.
            const auto * boundary_step = single_replica_plan_boundary->step.get();
            const auto * boundary_sorting_step = typeid_cast<const SortingStep *>(boundary_step);
            const bool output_is_replicated = typeid_cast<const LimitStep *>(boundary_step)
                || typeid_cast<const NegativeLimitStep *>(boundary_step)
                || (boundary_sorting_step && boundary_sorting_step->getLimit() != 0);
            const size_t output_replicas_divisor = output_is_replicated ? 1 : num_replicas;
            const auto local_plan_cost_estimation = stats->input_bytes / std::min<size_t>(max_threads, effective_max_reading_threads);
            const auto replicas_plan_cost_estimation
                = (stats->input_bytes / std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads)) + stats->output_bytes / output_replicas_divisor;
            /// Winning on time is necessary but not sufficient. Only the coordinated read is split; every
            /// other read of the subtree runs on each replica in full, so the cluster does
            /// `num_replicas` times that work for no change in wall-clock time - the comparison above
            /// cannot see it, because the term cancels on both sides. Decline a query that would spend
            /// more of the cluster on reading than the setting allows, however the time comparison turned
            /// out. The ratio is deliberately independent of `num_replicas`: it asks what share of the
            /// reading is duplicated, not how much work that adds up to, so adding replicas does not by
            /// itself make the query less likely to be distributed.
            const auto total_read_bytes = stats->input_bytes + stats->duplicated_bytes;
            const double duplicated_read_ratio
                = total_read_bytes ? static_cast<double>(stats->duplicated_bytes) / static_cast<double>(total_read_bytes) : 0.0;
            const bool replicas_plan_is_cheaper = local_plan_cost_estimation > replicas_plan_cost_estimation;
            const bool rejected_by_duplicated_reads = replicas_plan_is_cheaper
                && duplicated_read_ratio > static_cast<double>(optimization_settings.automatic_parallel_replicas_max_duplicated_read_ratio);
            const bool rejected_by_threshold = replicas_plan_is_cheaper
                && optimization_settings.automatic_parallel_replicas_min_bytes_per_replica
                && stats->input_bytes / num_replicas < optimization_settings.automatic_parallel_replicas_min_bytes_per_replica;
            /// Applying needs the replicas plan; the call with it counts the events.
            if (replicas_plan_is_cheaper && !rejected_by_duplicated_reads && !rejected_by_threshold && !plan_with_parallel_replicas)
                return true;

            ProfileEvents::increment(ProfileEvents::AutoParallelReplicasCostModelEvaluated);
            /// Unconditionally, before the decision. Recording it only when the threshold turns a
            /// query away would show only which queries a lower threshold would let through, never
            /// what a higher one would cost - and the queries it would start turning away are exactly
            /// the ones being applied here.
            ProfileEvents::increment(
                ProfileEvents::AutoParallelReplicasBytesPerReplica, stats->input_bytes / num_replicas);
            LOG_TRACE(
                getLogger("AutoParallelReplicas"),
                "The applied formula: {} / {} ? ({} / {} + {} / {}) ≡ {} ? {}",
                stats->input_bytes,
                std::min<size_t>(max_threads, effective_max_reading_threads),
                stats->input_bytes,
                std::min<size_t>(max_threads * num_replicas, effective_max_reading_threads),
                stats->output_bytes,
                output_replicas_divisor,
                local_plan_cost_estimation,
                replicas_plan_cost_estimation);
            if (rejected_by_duplicated_reads)
            {
                ProfileEvents::increment(ProfileEvents::AutoParallelReplicasRejectedByDuplicatedReads);
                LOG_TRACE(
                    getLogger("AutoParallelReplicas"),
                    "Parallel replicas are not used: {}x of the {} bytes read are read by every replica "
                    "({} duplicated against {} coordinated), above the allowed ratio of {}",
                    duplicated_read_ratio,
                    total_read_bytes,
                    stats->duplicated_bytes,
                    stats->input_bytes,
                    optimization_settings.automatic_parallel_replicas_max_duplicated_read_ratio);
                return false;
            }

            if (replicas_plan_is_cheaper)
            {
                if (rejected_by_threshold)
                {
                    /// The size is already recorded above, for every query that reaches the cost
                    /// model rather than only the ones turned away here.
                    ProfileEvents::increment(ProfileEvents::AutoParallelReplicasRejectedByThreshold);
                    LOG_TRACE(
                        getLogger("AutoParallelReplicas"),
                        "Not enabling parallel replicas reading because the read of {} bytes gives {} per replica, "
                        "less than automatic_parallel_replicas_min_bytes_per_replica {}",
                        stats->input_bytes,
                        stats->input_bytes / num_replicas,
                        optimization_settings.automatic_parallel_replicas_min_bytes_per_replica);
                    return false;
                }

                /// Every read of the candidate has to be given its analysis. One that is not would read
                /// every mark, so the candidate is worse than the plan it replaces; decline rather than run it.
                if (!transplantAnalysisToAllReads(*query_plan.getRootNode(), *plan_with_parallel_replicas->getRootNode()))
                {
                    /// `transplantAnalysisToAllReads` has logged which read could not be paired and why.
                    ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
                    return false;
                }
                /// The transplant pairs reads by name, while the read the decision stands on was found by
                /// walking down from the node matched by hash. Identical subtrees hash alike, so that node
                /// can be in another subtree than the one the replicas plan ships, and then the coordinated
                /// read was given the analysis of another read than the one the decision priced.
                if (reads->in_replicas_plan->getAnalyzedResult() != analysis)
                {
                    ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
                    LOG_TRACE(
                        getLogger("AutoParallelReplicas"),
                        "The coordinated read was paired with another single-node read than the one the decision "
                        "was made for. Skipping optimization");
                    return false;
                }
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

                if (!moveSetsFromLocalPlanToReplicasPlan(query_plan, *plan_with_parallel_replicas))
                {
                    ProfileEvents::increment(ProfileEvents::AutoParallelReplicasPlansNotComparable);
                    LOG_TRACE(
                        getLogger("AutoParallelReplicas"),
                        "A set of the plan with parallel replicas has no counterpart in the single-replica plan. "
                        "Skipping optimization");
                    return false;
                }
                LOG_TRACE(
                    getLogger("AutoParallelReplicas"),
                    "Replacing the plan with the parallel replicas one for hash {}",
                    single_replica_plan_boundary_hash);
                query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*plan_with_parallel_replicas));
                /// The one event that says the optimization changed this query. `ParallelReplicasQueryCount`
                /// does not: building the plan above increments it even when the plan is thrown away. Counted
                /// after the swap, not before: the swap can throw, and a query that failed on the way there
                /// did not get the optimization applied to it.
                ProfileEvents::increment(ProfileEvents::AutoParallelReplicasApplied);
                return false;
            }
            else if (!plan_with_parallel_replicas)
            {
                ProfileEvents::increment(ProfileEvents::AutoParallelReplicasRejectedByCostModel);
                LOG_TRACE(
                    getLogger("AutoParallelReplicas"),
                    "Parallel replicas are not used: decided from the statistics of {}, so no plan with parallel replicas was built",
                    single_replica_plan_boundary->step->getName());
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::AutoParallelReplicasRejectedByCostModel);
                LOG_TRACE(
                    getLogger("AutoParallelReplicas"),
                    "The cost model does not favour parallel replicas for hash {}. Not enabling parallel replicas reading",
                    single_replica_plan_boundary_hash);
            }
        }
    }
    else
    {
        /// Statistics are collected only with the replicas plan; the call with it counts the event.
        if (!plan_with_parallel_replicas)
            return true;
        ProfileEvents::increment(ProfileEvents::AutoParallelReplicasNoStatistics);
        LOG_TRACE(getLogger("AutoParallelReplicas"), "No stats found for hash {}", single_replica_plan_boundary_hash);
    }

    if (table_data_drifted_significantly
        || optimization_settings.automatic_parallel_replicas_mode == 2 // automatic_parallel_replicas_mode == 2 enforces statistics recollection
    )
    {
        /// Collect only at a boundary the replicas plan named. The cached one comes from a key that is approximate
        /// by design, and statistics collected at a wrong node would keep confirming it; going through the replicas
        /// plan here re-checks the mapping whenever the statistics are refreshed anyway.
        if (!plan_with_parallel_replicas)
            return true;

        auto statistics_block = std::make_shared<RuntimeDataflowStatisticsBlock>(single_replica_plan_boundary_hash, rows_to_read);
        auto updater = std::make_shared<RuntimeDataflowStatisticsCacheUpdater>(statistics_block);

        /// A replica whose partial aggregation uses memory-bound merging sorts its output by the group
        /// by keys before sending it, while this plan, which the sample is taken from, leaves it in
        /// hash-table order. The sample has to be priced in the replica's order, so tell the updater to
        /// sort it.
        ///
        /// The decision is read off the step that will actually run on the replicas rather than
        /// reconstructed from the settings, because the two parallel-replicas implementations turn bucket
        /// order on under different conditions: the query-based one via the `Planner` (`WithMergeableState`
        /// plus either `distributed_aggregation_memory_efficient` or
        /// `enable_memory_bound_merging_of_aggregation_results`), the plan-based one in
        /// `applyParallelReplicas` (`distributed_aggregation_memory_efficient` alone). Only the replica's
        /// own step knows which of the two built it.
        const auto * replicas_aggregating = typeid_cast<const AggregatingStep *>(replicas_plan_boundary->step.get());
        const auto * local_aggregating
            = typeid_cast<const AggregatingStep *>(single_replica_plan_boundary->step.get());
        if (replicas_aggregating && local_aggregating && replicas_aggregating->memoryBoundMergingWillBeUsed()
            && !local_aggregating->memoryBoundMergingWillBeUsed())
            updater->setReplicasSendOutputInKeyOrder();

        source_reading_step->setRuntimeDataflowStatisticsCacheUpdater(updater);
        single_replica_plan_boundary->step->setRuntimeDataflowStatisticsCacheUpdater(updater);
        /// Share the updater with the lazy half of the same read so its bytes land in the same
        /// `input_bytes`. Without it the statistics describe only the sorting column, while the lazy
        /// read is the larger of the two by far, and the cost model prices the query on a fraction of
        /// what replicas read.
        if (reads->lazy_to_instrument)
            reads->lazy_to_instrument->setRuntimeDataflowStatisticsCacheUpdater(updater);

        auto duplicated_reads_updater = std::make_shared<RuntimeDataflowStatisticsCacheUpdater>(statistics_block, /*duplicated=*/ true);
        for (auto * read_step : collectReadSteps(*single_replica_plan_boundary))
        {
            /// The coordinated read and its own lazy half are set up above, and their bytes are `input_bytes`.
            if (read_step == single_replica_plan_boundary->step.get() || read_step == source_reading_step
                || read_step == reads->lazy_to_instrument)
                continue;
            if (!read_step->supportsDataflowStatisticsCollection())
                continue;
            read_step->setRuntimeDataflowStatisticsCacheUpdater(duplicated_reads_updater);
        }
    }
    return false;
}

}
}
