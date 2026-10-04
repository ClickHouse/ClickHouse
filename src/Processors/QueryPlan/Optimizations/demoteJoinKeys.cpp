#include <Processors/QueryPlan/Optimizations/demoteJoinKeys.h>

#include <Core/Joins.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <limits>
#include <ranges>

namespace DB
{
namespace QueryPlanOptimizations
{

/// Helpers below are internal to this pass.
namespace
{

/// Nanoseconds of probe-time work one candidate row costs when an equality is checked during the
/// probe instead of being hashed into the key. Anchored on measurements of a FULL ALL join over
/// 10M rows: about 40 ns for a fixed-width key and about 290 ns for a String one. Both are
/// dominated by gathering the values out of the stored blocks through the row-ref lists, not by the
/// comparison itself, which is why the width of the key matters so much more than its type.
Float64 probeCostPerCandidateNs(const DataTypes & demoted_types, JoinKind kind, JoinStrictness strictness)
{
    Float64 cost = 0.0;
    for (const auto & type : demoted_types)
    {
        const auto & inner = removeNullable(removeLowCardinality(type));
        if (!inner->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            cost += 290.0;
        else if (inner->getSizeOfValueInMemory() <= sizeof(UInt64))
            cost += 8.0;
        else
            cost += 16.0;
    }

    /// An outer join pays for bookkeeping an inner join does not: a probe row whose whole bucket
    /// fails still has to be NULL-extended (`add_missing`, so no early exit), and for RIGHT/FULL
    /// every surviving candidate writes a used-flag for the non-joined pass (`need_flags`).
    if (isRightOrFull(kind))
        cost *= 1.5;
    else if (isLeft(kind))
        cost *= 1.2;

    /// ANY/SEMI/ANTI stop at the first surviving candidate rather than walking the whole bucket.
    if (strictness != JoinStrictness::All)
        cost *= 0.5;

    return cost;
}

/// Bytes of hash-table cells the demotion would avoid allocating: what the full key set needs,
/// minus what the kept subset needs. Both sides go through `HashJoin` so the cell size and the
/// power-of-two growth match the map that will actually be built, including the case where dropping
/// a key moves the whole key set into a narrower method. Returns a signed value because a narrower
/// key set can land on a *larger* array once rounding is taken into account.
/// Average width of one key of `nodes`, preferring what the build side measured over what the type
/// implies. Only consulted for a map variant that stores its keys out of line; 0 means unknown.
Float64 averageKeyBytes(
    const std::vector<const ActionsDAG::Node *> & nodes,
    const std::unordered_map<String, ColumnStats> & build_column_stats)
{
    Float64 total = 0.0;
    for (const auto * node : nodes)
    {
        auto it = build_column_stats.find(node->result_name);
        if (it != build_column_stats.end() && it->second.avg_bytes > 0.0)
            total += it->second.avg_bytes;
        else
            total += estimateColumnWidthFromType(*node->result_type);
    }
    return total;
}

Int64 estimatedTableBytesSaved(
    const auto & candidate,
    Float64 full_ndv,
    const std::vector<const ActionsDAG::Node *> & right_key_nodes,
    size_t equality_count,
    JoinStrictness strictness,
    const std::unordered_map<String, ColumnStats> & build_column_stats)
{
    DataTypes full_types;
    for (const auto * node : right_key_nodes)
        full_types.push_back(node->result_type);

    DataTypes kept_types;
    std::vector<const ActionsDAG::Node *> kept_nodes;
    for (size_t i : candidate.indices)
    {
        kept_types.push_back(right_key_nodes[i]->result_type);
        kept_nodes.push_back(right_key_nodes[i]);
    }

    if (full_types.size() != equality_count || kept_types.empty())
        return 0;

    /// Two-level maps hold the same cell type, so the choice only shifts how the rounding falls;
    /// ask for the single-level variant and accept that slack.
    const auto full_method = HashJoin::chooseMethodForTypes(full_types, /*use_two_level_maps=*/ false);
    const auto kept_method = HashJoin::chooseMethodForTypes(kept_types, /*use_two_level_maps=*/ false);

    const size_t full_bytes = HashJoin::estimateTableBytes(
        static_cast<size_t>(full_ndv), full_method, strictness,
        averageKeyBytes(right_key_nodes, build_column_stats));
    const size_t kept_bytes = HashJoin::estimateTableBytes(
        candidate.ndv, kept_method, strictness,
        averageKeyBytes(kept_nodes, build_column_stats));

    /// A variant whose cell size is unknown reports 0; do not turn that into a fictional saving.
    if (!full_bytes || !kept_bytes)
        return 0;

    return static_cast<Int64>(full_bytes) - static_cast<Int64>(kept_bytes);
}

}

/// Cardinality-driven optimization: when the equality keys of a JOIN together have a far higher NDV
/// than one of their subsets, build the hash table on that subset only and evaluate the remaining
/// equalities per row during the probe. This shrinks the hash table on multi-key joins whose trailing
/// keys are near-unique, e.g. `ON l.user_id = r.user_id AND l.request_id = r.request_id` where
/// `request_id` is unique per user: the table is then keyed on the `user_id` space instead of the
/// `user_id x request_id` space.
///
/// Runs here, while the reordered join is being emitted, rather than during physical conversion:
///   - the build-side row count and per-column NDVs are already known, so no extra
///     walk of the right subtree is needed;
///   - the demoted equalities leave `JoinOperator::expression` before
///     `deriveCacheKeysForNewJoin` runs, so the `HashTablesStatistics` cache key derived there
///     covers exactly the keys the hash table is built on, with no separate bookkeeping.
///
/// The demoted equalities move to `JoinOperator::probe_conditions`; `buildPhysicalJoinImpl` routes
/// those into `mixed_join_expression`, which is evaluated during the probe and therefore preserves
/// outer-join NULL-extension (a post-join filter would not).
///
/// Returns true if at least one equality was demoted.
bool demoteHighNdvKeysToProbe(
    JoinStepLogical & join_step,
    std::optional<UInt64> build_rows,
    const std::unordered_map<String, ColumnStats> & build_column_stats,
    const CachedSubsetNdvLookup & cached_subset_ndv,
    const MeasuredFanoutLookup & measured_fanout)
{
    const auto & join_settings = join_step.getJoinSettings();
    if (!join_settings.query_plan_hash_join_subset_keys_auto)
        return false;

    /// The demoted equality survives only as a mixed join expression, which just the hash family
    /// evaluates: `chooseJoinAlgorithm` rejects a mixed condition outright unless hash, parallel
    /// hash or grace hash is enabled, and an algorithm that is applicable but blind to mixed
    /// conditions (any merge flavour, `auto`, `direct`, IEJoin) would be picked first and silently
    /// drop the equality, producing extra rows. Demote only when every enabled algorithm evaluates
    /// it - an allowlist, so a newly added algorithm is excluded until it is known to support this.
    auto evaluates_mixed_conditions = [](JoinAlgorithm algorithm)
    {
        return algorithm == JoinAlgorithm::HASH
            || algorithm == JoinAlgorithm::PARALLEL_HASH
            || algorithm == JoinAlgorithm::GRACE_HASH
            /// Deprecated spelling of `direct,hash`: `tryDirectJoin` declines a mixed condition and
            /// the join falls through to `HashJoin`.
            || algorithm == JoinAlgorithm::DEFAULT;
    };
    /// One such algorithm being enabled is enough - the same condition `chooseJoinAlgorithm`
    /// itself asserts before it picks anything. Every other algorithm declines a mixed condition
    /// and falls through to the hash family rather than silently dropping it: `tryDirectJoin`
    /// returns nothing for one, `MergeJoin::isSupported` and `FullSortingMergeJoin::isSupported`
    /// both return false, and `AUTO` only builds a `JoinSwitcher` when `MergeJoin::isSupported`, so
    /// it degrades to `HashJoin` too. Requiring it of *every* enabled algorithm instead made the
    /// optimization unreachable under the default `join_algorithm`, which lists `direct` and
    /// `ie_join` alongside the hash family.
    if (!std::ranges::any_of(join_settings.join_algorithms, evaluates_mixed_conditions))
        return false;

    auto & join_operator = join_step.getJoinOperator();
    if (!HashJoin::isAdditionalFilterSupported(join_operator.kind, join_operator.strictness))
        return false;

    /// IEJoin is the one exception to the paragraph above, and it is a question of position rather
    /// than of applicability: listed first in `join_algorithm` it claims the join before the
    /// equalities are turned into hash keys, and it evaluates a mixed condition nowhere, so there
    /// is nothing to fall back to.
    if (isIEJoinPreferred(join_operator, join_settings))
        return false;

    if (!build_rows || *build_rows < join_settings.query_plan_hash_join_subset_keys_min_rows)
        return false;
    const Float64 rows = static_cast<Float64>(*build_rows);

    /// Positions in `join_operator.expression` of the plain cross-side equalities, with the build-side
    /// node of each. `NullSafeEquals` is left alone: the probe-time rewrite compares with `equals`,
    /// which does not match its semantics. Anything else (a residual predicate, a disjunction) is not
    /// a hash key to begin with.
    std::vector<size_t> equality_positions;
    std::vector<const ActionsDAG::Node *> right_key_nodes;
    for (size_t i = 0; i < join_operator.expression.size(); ++i)
    {
        auto [op, lhs, rhs] = join_operator.expression[i].asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        if (lhs.fromLeft() && rhs.fromRight())
            right_key_nodes.push_back(rhs.getNode());
        else if (lhs.fromRight() && rhs.fromLeft())
            right_key_nodes.push_back(lhs.getNode());
        else
            continue;
        equality_positions.push_back(i);
    }

    /// Demotion has to leave at least one hash key behind, so it needs at least two.
    if (equality_positions.size() < 2)
        return false;

    /// Candidate kept-key subsets, each with the NDV the hash table would have if built on it.
    /// Two sources:
    ///  (a) column statistics on the build side - one size-1 candidate per key with a known NDV;
    ///  (b) the `HashTablesStatistics` cache - a candidate for any subset some earlier query on this
    ///      same subtree happened to build a hash table on. That value is a measured joint NDV, so it
    ///      accounts for correlation between keys, which multiplying per-column NDVs cannot.
    struct Candidate
    {
        std::vector<size_t> indices;
        UInt64 ndv;
    };
    std::vector<Candidate> candidates;

    for (size_t i = 0; i < right_key_nodes.size(); ++i)
    {
        auto it = build_column_stats.find(right_key_nodes[i]->result_name);
        if (it == build_column_stats.end() || it->second.num_distinct_values == 0)
            continue;
        candidates.push_back({{i}, it->second.num_distinct_values});
    }

    if (cached_subset_ndv)
    {
        /// Enumerate subsets up to a small bound. Hits are sparse - only subsets the workload has
        /// actually built - so the cost is one SipHash plus one map probe per subset; the cap keeps
        /// the worst case bounded on joins with many equi keys.
        const size_t keys_count = right_key_nodes.size();
        const size_t max_subset_size = std::min<size_t>(keys_count, 4);
        std::vector<size_t> subset;
        std::vector<const ActionsDAG::Node *> subset_nodes;
        subset.reserve(max_subset_size);
        subset_nodes.reserve(max_subset_size);

        auto probe_subset = [&]
        {
            subset_nodes.clear();
            for (size_t i : subset)
                subset_nodes.push_back(right_key_nodes[i]);
            if (auto ndv = cached_subset_ndv(subset_nodes))
                candidates.push_back({subset, *ndv});
        };

        auto enumerate = [&](auto & self, size_t start, size_t depth_remaining) -> void
        {
            if (!subset.empty())
                probe_subset();
            if (depth_remaining == 0)
                return;
            for (size_t i = start; i < keys_count; ++i)
            {
                subset.push_back(i);
                self(self, i + 1, depth_remaining - 1);
                subset.pop_back();
            }
        };
        enumerate(enumerate, 0, max_subset_size);
    }

    if (candidates.empty())
        return false;

    /// The same subset can be scored by both sources. Keep the smallest NDV for it - over-stated
    /// distinctness would under-state the bucket size and make demotion look better than it is.
    std::ranges::sort(candidates, [](const auto & lhs, const auto & rhs)
    {
        if (lhs.indices != rhs.indices)
            return lhs.indices < rhs.indices;
        return lhs.ndv < rhs.ndv;
    });
    candidates.erase(
        std::unique(candidates.begin(), candidates.end(), [](const auto & lhs, const auto & rhs) { return lhs.indices == rhs.indices; }),
        candidates.end());

    /// Smallest hash table first, ties broken toward fewer kept keys (cheaper per-row hashing).
    std::ranges::sort(candidates, [](const auto & lhs, const auto & rhs)
    {
        if (lhs.ndv != rhs.ndv)
            return lhs.ndv < rhs.ndv;
        return lhs.indices.size() < rhs.indices.size();
    });

    /// Candidates must still discriminate to `rows * min_kept_selectivity` distinct values, so the
    /// probe-time equalities run over a bounded bucket rather than a large fraction of the table.
    const Float64 target_ndv = rows * join_settings.query_plan_hash_join_subset_keys_min_kept_selectivity;

    /// The NDV of the whole key set, which is what the hash table is keyed on today. It sizes the
    /// table the demotion would avoid building, so an over-estimate here inflates every candidate's
    /// apparent saving. Prefer a measured joint value; failing that take the largest single-key NDV
    /// rather than the product of them. The joint NDV lies between those two, and the lower end is
    /// the conservative choice: assuming the keys are independent would credit a demotion with
    /// removing a table far larger than the one that gets built.
    Float64 full_ndv = 0.0;
    for (const auto & candidate : candidates)
    {
        if (candidate.indices.size() == equality_positions.size())
        {
            full_ndv = static_cast<Float64>(candidate.ndv);
            break;
        }
        if (candidate.indices.size() == 1)
            full_ndv = std::max(full_ndv, static_cast<Float64>(candidate.ndv));
    }
    full_ndv = std::min(full_ndv, rows);

    /// Both sides of the trade, per candidate:
    ///
    ///  - cost: the mean bucket the kept keys leave, times what one candidate check costs for the
    ///    keys being demoted. Demoting can never make the probe cheaper - it only shrinks the hash
    ///    table - so this is what is being paid, and it is capped rather than merely compared.
    ///  - benefit: the cell array the full key set allocates minus the one the kept subset would.
    ///
    /// The cheapest surviving candidate wins. Picking the smallest-NDV one instead, as an earlier
    /// version did, systematically demotes the most discriminating key, which is exactly the choice
    /// that inflates the bucket the most.
    const Candidate * chosen = nullptr;
    Float64 chosen_cost = std::numeric_limits<Float64>::infinity();
    for (const auto & candidate : candidates)
    {
        if (static_cast<Float64>(candidate.ndv) < target_ndv)
            continue;
        if (candidate.indices.size() == equality_positions.size())
            continue;

        DataTypes demoted_types;
        std::vector<bool> in_candidate(right_key_nodes.size(), false);
        for (size_t i : candidate.indices)
            in_candidate[i] = true;
        for (size_t i = 0; i < right_key_nodes.size(); ++i)
        {
            if (!in_candidate[i])
                demoted_types.push_back(right_key_nodes[i]->result_type);
        }
        if (demoted_types.empty())
            continue;

        /// What one probe row will look at. The uniform estimate is a lower bound under skew, so
        /// prefer what a previous execution of this same key subset actually measured.
        Float64 fanout = rows / std::max(1.0, static_cast<Float64>(candidate.ndv));
        if (measured_fanout)
        {
            std::vector<const ActionsDAG::Node *> candidate_nodes;
            candidate_nodes.reserve(candidate.indices.size());
            for (size_t i : candidate.indices)
                candidate_nodes.push_back(right_key_nodes[i]);
            if (auto measured = measured_fanout(candidate_nodes))
                fanout = std::max(fanout, *measured);
        }

        const Float64 cost = fanout
            * probeCostPerCandidateNs(demoted_types, join_operator.kind, join_operator.strictness);
        if (cost > join_settings.query_plan_hash_join_subset_keys_max_probe_cost_ns)
            continue;

        const Int64 saving = estimatedTableBytesSaved(
            candidate, full_ndv, right_key_nodes, equality_positions.size(),
            join_operator.strictness, build_column_stats);
        if (saving < static_cast<Int64>(join_settings.query_plan_hash_join_subset_keys_min_saving_bytes))
            continue;

        if (cost < chosen_cost)
        {
            chosen = &candidate;
            chosen_cost = cost;
        }
    }
    if (!chosen)
        return false;

    std::vector<bool> kept(right_key_nodes.size(), false);
    for (size_t i : chosen->indices)
        kept[i] = true;

    /// Move the demoted equalities out of the ON expression, leaving every other condition
    /// (a non-equi predicate that already had to be evaluated during the join) where it was.
    std::vector<JoinActionRef> kept_expression;
    kept_expression.reserve(join_operator.expression.size());
    size_t equality_index = 0;
    size_t demoted_count = 0;
    for (size_t i = 0; i < join_operator.expression.size(); ++i)
    {
        if (equality_index >= equality_positions.size() || equality_positions[equality_index] != i)
        {
            kept_expression.push_back(join_operator.expression[i]);
            continue;
        }

        if (kept[equality_index])
            kept_expression.push_back(join_operator.expression[i]);
        else
        {
            join_operator.probe_conditions.push_back(join_operator.expression[i]);
            ++demoted_count;
        }
        ++equality_index;
    }
    join_operator.expression = std::move(kept_expression);

    LOG_DEBUG(
        getLogger("optimizeJoin"),
        "Demoted {} of {} JOIN equality keys to probe-time conditions (right_rows={}, kept_ndv={}, target_ndv={})",
        demoted_count, equality_positions.size(), *build_rows, chosen->ndv, target_ndv);

    return true;
}

}
}
