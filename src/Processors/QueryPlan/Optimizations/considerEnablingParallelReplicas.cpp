#include <Processors/QueryPlan/Optimizations/considerEnablingParallelReplicas.h>

#include <Core/Joins.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
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
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <cmath>
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

/// The rendezvous keys of every join runtime filter applied inside a branch, wherever the filter ended up:
/// pushed into the read's PREWHERE, or left as a `Filter` above it. Both are the same predicate doing the
/// same work, and only the filter's own counters say how much of it survived - the read's output row count
/// does not, since a filter above the read leaves that count untouched.
std::vector<String> collectRuntimeFilterKeys(const QueryPlan::Node & branch)
{
    std::vector<String> keys;

    auto collect_from_dag = [&](const ActionsDAG & dag)
    {
        for (const auto & node : dag.getNodes())
        {
            if (!node.is_runtime_filter_id)
                continue;
            if (const auto * column = typeid_cast<const ColumnConst *>(node.column.get()))
                keys.push_back(column->getValue<String>());
        }
    };

    Stack stack;
    traverseQueryPlan(
        stack,
        const_cast<QueryPlan::Node &>(branch),
        [&](auto & node)
        {
            if (const auto * filter = typeid_cast<const FilterStep *>(node.step.get()))
                collect_from_dag(filter->getExpression());
            else if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(node.step.get()))
            {
                if (const auto prewhere_info = source->getPrewhereInfo())
                    collect_from_dag(prewhere_info->prewhere_actions);
                if (const auto & filter_dag = source->getFilterActionsDAG())
                    collect_from_dag(*filter_dag);
            }
        });

    ::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    return keys;
}

struct JoinAboveFragment
{
    const JoinStep * step = nullptr;
    UInt64 node_hash = 0;
    const QueryPlan::Node * probe_side = nullptr;
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

    return JoinAboveFragment{
        .step = join_step,
        .node_hash = hash_it->second,
        .probe_side = join_node->children[0],
        .build_side = join_node->children[1]};
}

/// The name a join key has in the table it comes from: plan-level names carry the analyzer's `__tableN.`
/// qualifier, the storage's columns do not.
String unqualifiedColumnName(const String & name)
{
    const auto dot = name.find_last_of('.');
    return dot == String::npos ? name : name.substr(dot + 1);
}

/// What a read spends per row, split between the given columns and all of them. Prefers the compressed
/// sizes the storage tracks and falls back to the columns' own width where it has measured nothing yet - a
/// table whose parts were just written reports zeroes, and a missing measurement must not read as "free".
/// The fallback mixes in-memory width with compressed bytes, which over-states the column and so only ever
/// makes this decline shipping.
struct ReadBytesPerRow
{
    double of_columns = 0;
    double of_all = 0;
};

ReadBytesPerRow bytesPerRow(const ReadFromMergeTree & read, const NameSet & columns)
{
    const auto & data = read.getMergeTreeData();
    const auto sizes = data.getColumnSizes();
    const auto table_rows = data.getTotalActiveSizeInRows();
    const auto & columns_description = read.getStorageMetadata()->getColumns();

    ReadBytesPerRow result;
    for (const auto & name : read.getAllColumnNames())
    {
        double per_row = 0;
        if (table_rows)
            if (const auto it = sizes.find(name); it != sizes.end() && it->second.data_compressed)
                per_row = static_cast<double>(it->second.data_compressed) / static_cast<double>(table_rows);

        if (per_row == 0 && columns_description.hasPhysical(name))
        {
            const auto type = columns_description.getPhysical(name).type;
            /// A variable-width column has no size to ask for; 16 bytes is a placeholder that keeps such a
            /// column from counting as nothing.
            per_row = type->haveMaximumSizeOfValue() ? static_cast<double>(type->getMaximumSizeOfValueInMemory()) : 16.0;
        }

        result.of_all += per_row;
        if (columns.contains(name))
            result.of_columns += per_row;
    }
    return result;
}

/// The share of a read's bytes that these columns account for. This is what separates a key-range
/// restriction from a PREWHERE: the predicate's own column is read for every row either way, everything
/// else only for the rows that survive it.
double columnShareOfRead(const ReadFromMergeTree & read, const NameSet & columns)
{
    const auto per_row = bytesPerRow(read, columns);
    return per_row.of_all > 0 ? per_row.of_columns / per_row.of_all : 0.0;
}

/// What shipping costs the initiator: one pass over the build side's key columns to fill the set. The join
/// reads the build side regardless, so only this extra pass is chargeable to the decision.
std::optional<size_t> buildSideKeyScanBytes(const QueryPlan::Node & build_side, const NameSet & key_columns)
{
    ReadFromMergeTree * read = findReadingStep(build_side);
    if (!read)
        return {};

    const auto analysis = read->getAnalyzedResult() ? read->getAnalyzedResult() : read->selectRangesToRead();
    if (!analysis)
        return {};

    const auto per_row = bytesPerRow(*read, key_columns);
    if (per_row.of_columns <= 0)
        return {};

    return static_cast<size_t>(per_row.of_columns * static_cast<double>(analysis->selected_rows));
}

/// What shipping the join's semi-join predicate would do to the replicas' cost, in the same bytes the rest
/// of the cost model is expressed in.
struct ShippedPredicateEstimate
{
    /// The fraction of the fragment's rows the predicate keeps, end to end.
    double match_rate = 1.0;
    /// The fraction of them the join's runtime filter already kept when the statistics were measured.
    double filter_pass_rate = 1.0;
    /// The fraction of the fragment's input bytes a read keeps under a rate: `shipped` under the whole
    /// predicate, `measured` under the part of it the runtime filter had already applied.
    double read_share_shipped = 1.0;
    double read_share_measured = 1.0;
    /// What the initiator pays once, to build the set.
    size_t build_scan_bytes = 0;
    /// Whether the predicate acts on the read's leading key column, so it can skip granules outright.
    bool restricts_key_range = false;
};

/// Prices shipping without building anything. It has to: building the plan that ships the predicate
/// materializes the set, and a "no" afterwards does not refund that scan.
///
/// The rate the predicate keeps is split between two places, because a join runtime filter applies the very
/// same predicate earlier. The filter applies below the aggregation, so whatever it removed the join never
/// sees and the join reports almost nothing but matches; without a filter nothing is removed early and the
/// join's rate is the whole story. Their product is the fraction either way, and it is a product rather than
/// a choice because the filter is approximate above `join_runtime_filter_exact_values_limit` - it passes
/// false positives that the join then rejects.
std::optional<ShippedPredicateEstimate> estimateShippedPredicate(
    const JoinAboveFragment & join, const ReadFromMergeTree & fragment_read)
{
    /// Keyed by the join's own node hash, not by the fragment's: the same aggregated subquery can appear
    /// under different joins, and those queries share the fragment's entry.
    const auto join_stats = getRuntimeDataflowStatisticsCache().getStats(join.node_hash);
    if (!join_stats || !join_stats->join_probe_rows)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "No join match rate was measured, not shipping the join predicate");
        return {};
    }

    NameSet probe_key_columns;
    NameSet build_key_columns;
    for (const auto & clause : join.step->getJoin()->getTableJoin().getClauses())
    {
        for (const auto & name : clause.key_names_left)
            probe_key_columns.insert(unqualifiedColumnName(name));
        for (const auto & name : clause.key_names_right)
            build_key_columns.insert(unqualifiedColumnName(name));
    }
    if (probe_key_columns.empty() || build_key_columns.empty())
        return {};

    const auto build_scan_bytes = buildSideKeyScanBytes(*join.build_side, build_key_columns);
    if (!build_scan_bytes)
    {
        LOG_DEBUG(getLogger("optimizeTree"), "Cannot size the join's build side, not shipping the join predicate");
        return {};
    }

    const double filter_pass_rate = join_stats->filter_checked_rows
        ? std::min(1.0, static_cast<double>(join_stats->filter_passed_rows) / static_cast<double>(join_stats->filter_checked_rows))
        : 1.0;
    const double join_match_rate
        = static_cast<double>(join_stats->join_matched_probe_rows) / static_cast<double>(join_stats->join_probe_rows);

    ShippedPredicateEstimate estimate;
    estimate.match_rate = filter_pass_rate * join_match_rate;
    estimate.filter_pass_rate = filter_pass_rate;
    estimate.build_scan_bytes = *build_scan_bytes;

    /// Only on the *leading* key column does a predicate skip granules outright; deeper in the key it can
    /// only narrow ranges it shares with the columns before it, which the data rarely honors - measured on
    /// TPC-H, an `l_orderkey` set against a key of `(l_shipdate, l_orderkey, ...)` pruned nothing at all.
    /// Anything else is priced as what it certainly is, a PREWHERE: the key column is still read for every
    /// row, the payload only for the rows that survive.
    const auto primary_key = fragment_read.getStorageMetadata()->getPrimaryKeyColumns();
    estimate.restricts_key_range = !primary_key.empty() && probe_key_columns.contains(primary_key.front());
    if (estimate.restricts_key_range)
    {
        estimate.read_share_shipped = estimate.match_rate;
        estimate.read_share_measured = filter_pass_rate;
    }
    else
    {
        /// PREWHERE skips the payload columns per granule, not per row, so it saves nothing unless whole
        /// granules lose every row. Treating the surviving rows as spread evenly, a granule survives with
        /// probability `1 - (1 - match_rate) ^ rows_per_granule`, which for anything but a very small rate
        /// is 1: the key column is read for every row and the payload with it. Where the survivors are
        /// clustered instead - a date range mapping onto a key range - this under-states the saving, which
        /// is the safe direction: it costs a shipped predicate that would have paid off, not a query.
        const double key_share = columnShareOfRead(fragment_read, probe_key_columns);
        const auto analysis = fragment_read.getAnalyzedResult();
        const double rows_per_granule
            = analysis && analysis->selected_marks ? static_cast<double>(analysis->selected_rows) / static_cast<double>(analysis->selected_marks) : 8192.0;
        const auto share_under = [&](double rate)
        {
            const double granule_survival = 1.0 - std::pow(1.0 - rate, rows_per_granule);
            return key_share + (1.0 - key_share) * std::min(1.0, granule_survival);
        };
        estimate.read_share_shipped = share_under(estimate.match_rate);
        estimate.read_share_measured = share_under(filter_pass_rate);
    }

    LOG_DEBUG(
        getLogger("optimizeTree"),
        "Shipping the join predicate leaves a read {} of its bytes where the measured plan already kept {} "
        "({} the key range), against a {} byte scan to build the set (match rate {}: {} of {} rows past the "
        "join's runtime filters, {}/{} matched by the join)",
        estimate.read_share_shipped,
        estimate.read_share_measured,
        estimate.restricts_key_range ? "restricting" : "not restricting",
        estimate.build_scan_bytes,
        estimate.match_rate,
        join_stats->filter_passed_rows,
        join_stats->filter_checked_rows,
        join_stats->join_matched_probe_rows,
        join_stats->join_probe_rows);

    return estimate;
}

/// Whether re-running index analysis with the shipped predicate could actually restrict what the replicas
/// read. Only the key condition knows: the predicate prunes when its column is usable by the primary key,
/// and nothing else about it - not its selectivity, not the data - decides that.
///
/// When it cannot prune, the single-node index analysis is still exactly right for this read, so it is
/// reused and the expensive part is skipped. The predicate is still applied, wherever the optimizer put it:
/// moved into PREWHERE, or left as a filter above the read. Only the range selection is avoided.
bool shippedPredicateCanRestrictRanges(const ReadFromMergeTree & read)
{
    const auto & indexes = read.getIndexes();
    if (!indexes || !indexes->key_condition)
        return false;

    return !indexes->key_condition->generateUnsubstituted().alwaysUnknownOrTrue();
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
            const auto reading_threads = [&](size_t bytes, size_t thread_budget)
            {
                const auto effective = optimization_settings.min_bytes_per_task_for_reading
                    ? bytes / optimization_settings.min_bytes_per_task_for_reading + 1
                    : SIZE_MAX;
                return std::min<size_t>(thread_budget, effective);
            };
            const auto num_replicas = optimization_settings.max_parallel_replicas;
            const auto local_plan_cost_estimation = stats->input_bytes / reading_threads(stats->input_bytes, max_threads);
            auto replicas_plan_cost_estimation
                = (stats->input_bytes / reading_threads(stats->input_bytes, max_threads * num_replicas)) + stats->output_bytes / num_replicas;
            LOG_DEBUG(
                getLogger("optimizeTree"),
                "The applied formula: {} / {} ? ({} / {} + {} / {}) ≡ {} ? {}",
                stats->input_bytes,
                reading_threads(stats->input_bytes, max_threads),
                stats->input_bytes,
                reading_threads(stats->input_bytes, max_threads * num_replicas),
                stats->output_bytes,
                num_replicas,
                local_plan_cost_estimation,
                replicas_plan_cost_estimation);

            /// The same comparison for the plan that ships the join predicate, made here rather than after
            /// the parallel-replicas decision. Shipping shrinks the very term that makes replicas look
            /// unattractive - the groups the fragment sends back - so deciding replicas first and shipping
            /// second rejects exactly the queries shipping is for. Measured on TPC-H sf=100: a `lineitem`
            /// aggregate joined to one month of `orders` was declined at this point (145958180 ? 185013300)
            /// while the shipped plan ran it in 635ms against the 1425ms the unshipped decision settled for.
            ///
            /// The initiator's scan for the set is part of the shipped plan's cost, so a predicate that saves
            /// less than it costs to build simply loses the comparison; there is no separate threshold.
            std::optional<ShippedPredicateEstimate> ship;
            auto shipped_replicas_plan_cost_estimation = std::numeric_limits<size_t>::max();
            if (!manual_ship_join_predicate)
            {
                if (const auto join_above_fragment
                    = findJoinAboveFragment(root, *corresponding_node_in_single_replica_plan, single_replica_plan_hashes))
                {
                    ship = estimateShippedPredicate(*join_above_fragment, *source_reading_step);
                    if (ship)
                    {
                        /// The statistics were measured on the single-node plan, and there the join's runtime
                        /// filter had already applied this very predicate below the aggregation. They describe
                        /// a plan that filters, which is the plan that ships the predicate - not the one that
                        /// does not. Replicas without the predicate read what the filter skipped and aggregate
                        /// the groups it removed, so their cost is the measured cost scaled back up. Where no
                        /// filter ran the rates are 1 and only the shipped side moves, as before.
                        const auto scaled = [](size_t bytes, double from, double to)
                        {
                            /// A rate of zero would scale to infinity; it also means the measurement saw
                            /// nothing pass, where any estimate is guesswork.
                            return static_cast<size_t>(static_cast<double>(bytes) * to / std::max(from, 1e-6));
                        };
                        const auto unshipped_input_bytes = scaled(stats->input_bytes, ship->read_share_measured, 1.0);
                        const auto unshipped_output_bytes = scaled(stats->output_bytes, ship->filter_pass_rate, 1.0);
                        const auto shipped_input_bytes
                            = scaled(stats->input_bytes, ship->read_share_measured, ship->read_share_shipped);
                        const auto shipped_output_bytes = scaled(stats->output_bytes, ship->filter_pass_rate, ship->match_rate);

                        replicas_plan_cost_estimation
                            = unshipped_input_bytes / reading_threads(unshipped_input_bytes, max_threads * num_replicas)
                            + unshipped_output_bytes / num_replicas;
                        shipped_replicas_plan_cost_estimation
                            = shipped_input_bytes / reading_threads(shipped_input_bytes, max_threads * num_replicas)
                            + shipped_output_bytes / num_replicas
                            + ship->build_scan_bytes;
                        LOG_DEBUG(
                            getLogger("optimizeTree"),
                            "Priced against a fragment the replicas cannot filter: without the predicate {} / {} + {} / {} ≡ {}, "
                            "with it shipped {} / {} + {} / {} + {} ≡ {}",
                            unshipped_input_bytes,
                            reading_threads(unshipped_input_bytes, max_threads * num_replicas),
                            unshipped_output_bytes,
                            num_replicas,
                            replicas_plan_cost_estimation,
                            shipped_input_bytes,
                            reading_threads(shipped_input_bytes, max_threads * num_replicas),
                            shipped_output_bytes,
                            num_replicas,
                            ship->build_scan_bytes,
                            shipped_replicas_plan_cost_estimation);
                    }
                }
            }

            const auto best_replicas_plan_cost_estimation
                = std::min(replicas_plan_cost_estimation, shipped_replicas_plan_cost_estimation);
            if (local_plan_cost_estimation > best_replicas_plan_cost_estimation)
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
                bool shipped_predicate_restricts_ranges = false;
                /// Shipping is not free beyond the scan already priced above: a subquery to plan, a set to
                /// materialize, a temporary table to send. None of that is in the estimate, so a predicate
                /// that only just wins on paper loses in practice - measured at 1.1x to 1.7x slower on the
                /// shapes where the two costs came out within a percent of each other. Ask for a margin
                /// wide enough that the fixed cost cannot swallow the gain.
                if (shipped_replicas_plan_cost_estimation * 11 / 10 < replicas_plan_cost_estimation)
                {
                    /// Plain `in`, not `globalIn`. The set is materialized once on the initiator either
                    /// way - `buildQueryTreeForShard` ships the set of an injected `IN` as a temporary
                    /// table, so no replica repeats the scan of the build side - but `globalIn` would cost
                    /// the replicas their PREWHERE, which `MergeTreeWhereOptimizer::cannotBeMoved` refuses
                    /// to move, and they would read every column for every row instead of the key first.
                    auto plan_with_shipped_predicate = optimization_settings.query_plan_with_parallel_replicas_builder(1);
                    const auto * shipped_final_node
                        = plan_with_shipped_predicate ? findTopNodeOfReplicasPlan(plan_with_shipped_predicate->getRootNode()) : nullptr;

                    if (shipped_final_node)
                    {
                        ReadFromMergeTree * shipped_reading_step = findReadingStep(*shipped_final_node);
                        plan_with_parallel_replicas = std::move(plan_with_shipped_predicate);
                        final_node_in_replica_plan = shipped_final_node;
                        shipped_join_predicate = true;
                        /// Priced as a key-range restriction above only when the predicate is on the leading
                        /// key column; the plan that now exists is what decides whether the analysis below
                        /// may be reused, so ask it rather than the estimate.
                        shipped_predicate_restricts_ranges
                            = shipped_reading_step && shippedPredicateCanRestrictRanges(*shipped_reading_step);
                        LOG_DEBUG(getLogger("optimizeTree"), "Shipping the join predicate into the replicas' fragment");
                    }
                    else if (local_plan_cost_estimation <= replicas_plan_cost_estimation)
                    {
                        /// Replicas were only worth it with the predicate shipped, and there is no plan that
                        /// ships it. Running them without it is a plan this function already priced as worse
                        /// than staying on one node.
                        LOG_DEBUG(
                            getLogger("optimizeTree"),
                            "The plan with the join predicate shipped has no parallel replicas fragment, and without it "
                            "parallel replicas do not pay off");
                        return;
                    }
                    else
                    {
                        LOG_DEBUG(
                            getLogger("optimizeTree"),
                            "The plan with the join predicate shipped has no parallel replicas fragment, keeping the plain one");
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
                if (shipped_join_predicate && shipped_predicate_restricts_ranges)
                {
                    LOG_DEBUG(
                        getLogger("optimizeTree"),
                        "Not reusing the single-node index analysis: the shipped predicate can restrict the key range, "
                        "and the analysis to reuse was made before the predicate existed");
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
        {
            join_above_fragment->step->recordProbeMatchRateInto(updater, join_above_fragment->node_hash);

            /// The join's match rate alone cannot price the predicate when a join runtime filter already
            /// applies it below the aggregation: everything that reaches the join then matches. Read that
            /// part of the selectivity off the filters themselves, once they have run.
            if (auto filter_keys = collectRuntimeFilterKeys(*join_above_fragment->probe_side); !filter_keys.empty())
            {
                updater->setJoinRuntimeFilterPassRateProvider(
                    [lookup = source_reading_step->getContext()->getRuntimeFilterLookup(), keys = std::move(filter_keys)]
                    {
                        /// The filters are applied as one conjunction over the same rows, so no more rows
                        /// survive them all than survive the strongest of them. Taking that one - rather
                        /// than a product, which would assume the keys are independent - keeps the estimate
                        /// on the side that under-states how much the predicate removes.
                        RuntimeFilterPassRate strongest;
                        for (const auto & key : keys)
                        {
                            const auto filter = lookup->find(key);
                            if (!filter)
                                continue;

                            const auto & stats = filter->getStats();
                            /// Rows the filter waved through while it was disabled for performing poorly
                            /// count as checked and passed: they did meet the predicate's cost and none of
                            /// them was removed.
                            const auto skipped = static_cast<size_t>(stats.rows_skipped.load(std::memory_order_relaxed));
                            const RuntimeFilterPassRate rate{
                                .checked_rows = static_cast<size_t>(stats.rows_checked.load(std::memory_order_relaxed)) + skipped,
                                .passed_rows = static_cast<size_t>(stats.rows_passed.load(std::memory_order_relaxed)) + skipped};

                            if (!rate.checked_rows)
                                continue;
                            if (!strongest.checked_rows
                                || static_cast<double>(rate.passed_rows) * static_cast<double>(strongest.checked_rows)
                                    > static_cast<double>(strongest.passed_rows) * static_cast<double>(rate.checked_rows))
                                strongest = rate;
                        }
                        return strongest;
                    });
            }
        }
    }
}

}
}
