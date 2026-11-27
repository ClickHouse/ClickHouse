#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>
#include <Processors/QueryPlan/Optimizations/joinOrderDP.h>

#include <Interpreters/ProcessList.h>

#include <unordered_set>
#include <utility>
#include <vector>
#include <fmt/ranges.h>

namespace DB
{

namespace
{

class DPHypJoinOrderOptimizer
{
public:
    DPHypJoinOrderOptimizer(
        QueryGraph & query_graph_,
        UInt64 max_searched_plans_,
        QueryStatusPtr query_status_,
        std::function<bool()> interactive_cancel_callback_)
        : query_graph(query_graph_)
        , max_searched_plans(max_searched_plans_)
        , query_status(std::move(query_status_))
        , interactive_cancel_callback(std::move(interactive_cancel_callback_))
    {
    }

    DPJoinEntryPtr solve();

private:
    /// Periodically called from potentially long running optimization to check time limits and send progress
    void checkLimits();

    /// Polled inside the DPhyp enumeration loops. Returns false to stop enumeration when a partial plan
    /// cannot be handled or the search budget is exhausted, so `solve` returns nullptr and the next
    /// algorithm in the chain runs. Throws via `checkLimits` on query timeout or cancellation.
    bool continueEnumeration();

    /// Try to build the best join plan between left_rels and right_rels.
    /// Updates dp_table if a better plan is found.
    void tryJoin(const BitSet & left_rels, const BitSet & right_rels);

    /// DPhyp helpers
    void buildHyperedges();
    BitSet getNeighborhood(const BitSet & node_set) const;

    /// DPhyp enumeration functions from "Dynamic Programming Strikes Back"
    /// (Moerkotte & Neumann, SIGMOD 2008), Section 3.
    void emitCsg(const BitSet & csg);                       /// Generate complement seeds for a connected subgraph
    void enumerateCsgRec(const BitSet & csg, const BitSet & exclusion); /// Grow the primary connected subgraph
    void emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg); /// Evaluate a csg-cmp pair
    void enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion); /// Grow the complement

    QueryGraph & query_graph;
    SelectivityCache expression_selectivity;
    PlanMemo dp_table;

    /** A hyperedge in the join graph connecting a set of left relations to a set of right relations.
    * For simple binary predicates (A.x = B.y), |left| = |right| = 1.
    * For complex predicates (A.x = B.y + C.z), left and/or right may span multiple relations.
    */
    struct Hyperedge
    {
        BitSet left;
        BitSet right;
    };

    /// DPhyp hyperedge representation (built lazily by buildHyperedges)
    std::vector<Hyperedge> hyperedges;
    std::vector<std::vector<size_t>> node_to_edge_ids; /// node index -> hyperedge indices

    /// Set by `tryJoin` when it encounters a single-table or constant predicate inside the join edges
    /// that `dphyp` does not yet know how to attach. `solve` returns `nullptr` so the fallback
    /// algorithm chain (e.g. `dphyp,greedy`) can produce a valid plan.
    bool dphyp_unsupported_predicate = false;

    /// Number of partial plans enumerated so far and the deterministic budget that bounds it.
    /// When the budget is exceeded the current solver gives up and returns `nullptr` so the next
    /// algorithm in the chain runs. Both are reset at the start of each solver.
    size_t searched_plans = 0;
    bool search_budget_exceeded = false;
    const UInt64 max_searched_plans;

    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

void DPHypJoinOrderOptimizer::checkLimits()
{
    if (query_status)
        query_status->checkTimeLimit();
    if (interactive_cancel_callback)
        interactive_cancel_callback();
}

bool DPHypJoinOrderOptimizer::continueEnumeration()
{
    if (dphyp_unsupported_predicate || search_budget_exceeded)
        return false;
    ++searched_plans;
    if (max_searched_plans && searched_plans > max_searched_plans)
    {
        search_budget_exceeded = true;
        LOG_TRACE(log, "Exceeded the limit of {} searched plans, falling back", max_searched_plans);
        return false;
    }
    /// `checkLimits` invokes the interactive cancel callback, which can send progress over the
    /// network and snapshot profile events. Poll it once every few thousand enumerated subsets
    /// instead of on every one, which would otherwise dominate the optimization time.
    if ((searched_plans & 0xFFF) == 0)
        checkLimits();
    return true;
}

void DPHypJoinOrderOptimizer::tryJoin(const BitSet & left_rels, const BitSet & right_rels)
{
    auto left_entry = dp_table.find(left_rels);
    if (left_entry == dp_table.end())
        return;

    auto right_entry = dp_table.find(right_rels);
    if (right_entry == dp_table.end())
        return;

    auto join_kind = isValidJoinOrder(query_graph, left_rels, right_rels);
    if (!join_kind)
        return;

    /// Restrict to inner joins for now (same as DPsize FIXME)
    if (*join_kind != JoinKind::Inner)
        return;

    auto applicable_predicates = getApplicableExpressions(query_graph, left_rels, right_rels);
    std::vector<JoinActionRef *> connecting_predicates;
    for (auto * predicate : applicable_predicates)
    {
        if (connects(predicate, left_rels, right_rels))
        {
            connecting_predicates.push_back(predicate);
            continue;
        }

        /// Predicates spanning 2+ relations were already applied in a sub-join.
        /// Single-table or constant predicates (e.g. moved into `ON` by
        /// `query_plan_merge_filter_into_join_condition`) are not handled by `dphyp` here;
        /// `dpsize` attaches them at the smallest containing join, but `dphyp` would need
        /// extra bookkeeping to avoid double-application. For now, mark the query as
        /// unsupported and let `solve` return `nullptr` so the fallback chain runs.
        if (predicate->getSourceRelations().count() < 2)
        {
            LOG_TRACE(log, "DPhyp cannot attach non-connecting predicate {} (sources: {{ {} }}), falling back",
                predicate->dump(), fmt::join(predicate->getSourceRelations(), ","));
            dphyp_unsupported_predicate = true;
            return;
        }
    }

    /// When no explicit predicate connects the two sides, check transitive connectivity
    /// via column equivalence classes (e.g. A.key=B.key AND B.key=C.key implies A.key=C.key).
    /// `cleanupJoinPredicates` will synthesize the missing predicate after optimization.
    if (connecting_predicates.empty()
        && !query_graph.areTransitivelyConnected(left_rels, right_rels))
        return;

    evaluateJoin(query_graph, dp_table, expression_selectivity, left_entry->second, right_entry->second, *join_kind, connecting_predicates, log);
}

/// Build the hyperedge representation of the join graph used by DPhyp.
/// Each join predicate becomes a hyperedge (left_rels, right_rels).
/// Column equivalence classes add synthetic edges for transitively-connected pairs.
/// The adjacency index `node_to_edge_ids` maps each relation to the hyperedges that touch it.
void DPHypJoinOrderOptimizer::buildHyperedges()
{
    const size_t num_relations = query_graph.relation_stats.size();
    node_to_edge_ids.assign(num_relations, {});
    hyperedges.clear();

    auto add_hyperedge = [&](const BitSet & left_rels, const BitSet & right_rels)
    {
        size_t hyperedge_id = hyperedges.size();
        hyperedges.push_back({left_rels, right_rels});

        for (auto rel : left_rels)
            if (rel < num_relations)
                node_to_edge_ids[rel].push_back(hyperedge_id);
        for (auto rel : right_rels)
            if (rel < num_relations && !left_rels.test(rel))
                node_to_edge_ids[rel].push_back(hyperedge_id);
    };

    /// Phase 1: create hyperedges from explicit join predicates.
    /// Duplicate edges for the same relation pair (e.g. A.x=B.x AND A.y=B.y) are harmless:
    /// `getNeighborhood` ORs results into a BitSet, and `tryJoin` collects predicates
    /// from `query_graph.edges`, not from hyperedges.
    for (const auto & predicate : query_graph.edges)
    {
        if (!predicate)
            continue;

        BitSet left_rels;
        BitSet right_rels;

        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Unknown && lhs && rhs)
        {
            left_rels  = lhs.getSourceRelations();
            right_rels = rhs.getSourceRelations();
        }
        else
        {
            /// Non-binary predicate: treat the full source set as both endpoints.
            left_rels  = predicate.getSourceRelations();
            right_rels = predicate.getSourceRelations();
        }

        if (!left_rels.any() || !right_rels.any())
            continue;

        add_hyperedge(left_rels, right_rels);
    }

    /// Phase 2: add synthetic hyperedges for transitively-connected relation pairs.
    /// Column equivalence classes (e.g. A.key=B.key AND B.key=C.key implies A.key=C.key)
    /// connect relations that have no direct predicate. Without these edges DPhyp's
    /// neighborhood traversal would never discover the pair.

    /// Build a connectivity matrix from explicit edges to avoid duplicating them.
    std::vector<BitSet> connected_rels(num_relations);
    for (const auto & hyperedge : hyperedges)
    {
        auto left_rel = hyperedge.left.getSingleBit();
        auto right_rel = hyperedge.right.getSingleBit();
        if (left_rel && right_rel)
        {
            connected_rels[*left_rel].set(*right_rel);
            connected_rels[*right_rel].set(*left_rel);
        }
    }

    using ConstClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;
    std::unordered_set<ConstClassPtr> processed_classes;

    for (const auto & [member, equiv_class] : query_graph.column_equivalences.getMemberToClassMap())
    {
        if (!equiv_class || !processed_classes.insert(equiv_class).second)
            continue;

        /// Collect all distinct relations in this equivalence class.
        BitSet seen_rels;
        std::vector<size_t> class_rels;
        for (const auto & column : *equiv_class)
        {
            auto relation = column.getSourceRelations().getSingleBit();
            if (relation && *relation < num_relations && !seen_rels.test(*relation))
            {
                seen_rels.set(*relation);
                class_rels.push_back(*relation);
            }
        }

        for (size_t i = 0; i < class_rels.size(); ++i)
        {
            for (size_t j = i + 1; j < class_rels.size(); ++j)
            {
                if (connected_rels[class_rels[i]].test(class_rels[j]))
                    continue;

                connected_rels[class_rels[i]].set(class_rels[j]);
                connected_rels[class_rels[j]].set(class_rels[i]);

                BitSet left_singleton;
                BitSet right_singleton;
                left_singleton.set(class_rels[i]);
                right_singleton.set(class_rels[j]);
                add_hyperedge(left_singleton, right_singleton);
            }
        }
    }
}

/// Returns the set of all relations adjacent to `node_set` via any hyperedge,
/// excluding `node_set` itself.
///
/// A hyperedge (L, R) represents a join predicate with left sources L and right sources R.
/// For example, `A.x = B.y` gives L={A}, R={B}; `A.x + B.y = C.z` gives L={A,B}, R={C}.
/// R is reachable from node_set when L is fully contained in node_set (and vice versa).
///
/// Non-binary predicates like `f(A,B,C) = const` are represented as L={A,B,C}, R={A,B,C}
BitSet DPHypJoinOrderOptimizer::getNeighborhood(const BitSet & node_set) const
{
    BitSet neighbors;
    BitSet visited_edges;
    for (auto node : node_set)
    {
        if (node >= node_to_edge_ids.size())
            continue;
        for (auto hyperedge_id : node_to_edge_ids[node])
        {
            if (visited_edges.test(hyperedge_id))
                continue;
            visited_edges.set(hyperedge_id);
            const auto & edge = hyperedges[hyperedge_id];
            if (edge.left == edge.right)
            {
                /// In case of non-binary predicate (`f(A,B,C) = const`) the hyperedge is
                /// represented as L={A,B,C}, R={A,B,C}
                neighbors |= edge.left;
            }
            else
            {
                if (isSubsetOf(edge.left, node_set))
                    neighbors |= edge.right;
                if (isSubsetOf(edge.right, node_set))
                    neighbors |= edge.left;
            }
        }
    }
    auto result = neighbors.andNot(node_set);
    LOG_TEST(log, "DPhyp: getNeighborhood({}) = {}",
        fmt::join(node_set, ","), fmt::join(result, ","));
    return result;
}

/// Enumerate all non-empty subsets of `mask`, calling `func` for each.
/// Uses an integer bitmask over the positions of set bits in `mask`.
template <typename F>
void forEachNonEmptySubset(const BitSet & mask, F && func)
{
    std::vector<size_t> bit_positions;
    for (auto bit : mask)
        bit_positions.push_back(bit);

    const size_t num_bits = bit_positions.size();
    if (num_bits == 0)
        return;
    chassert(num_bits < 64);

    const UInt64 num_subsets = 1ULL << num_bits;
    for (UInt64 subset_mask = 1; subset_mask < num_subsets; ++subset_mask)
    {
        BitSet subset;
        for (size_t i = 0; i < num_bits; ++i)
            if (subset_mask & (1ULL << i))
                subset.set(bit_positions[i]);
        /// The callback returns false to stop enumeration early.
        if (!func(subset))
            return;
    }
}

/// The four functions below implement the core DPhyp enumeration from
/// "Dynamic Programming Strikes Back" (Moerkotte & Neumann, SIGMOD 2008), Section 3.
///
/// `emitCsg` (paper: EmitCsg, Sec 3.3) -- given a connected subgraph S1, generates all
///     complement seeds S2 = {v} from the neighborhood and extends them via `enumerateCmpRec`.
/// `enumerateCmpRec` (paper: EnumerateCmpRec, Sec 3.4) -- recursively extends complement S2
///     by adding neighboring nodes, emitting each valid csg-cmp pair.
/// `enumerateCsgRec` (paper: EnumerateCsgRec, Sec 3.2) -- recursively extends the primary
///     connected subgraph S1 by adding neighboring nodes.
/// `emitCsgCmp` (paper: EmitCsgCmp, Sec 3.5) -- evaluates a (S1, S2) pair for plan construction.
///
/// Deviation from the paper: EmitCsg checks connectivity (existence of a hyperedge
/// connecting S1 and S2) before calling EmitCsgCmp. We skip this check here and let
/// `tryJoin` handle it, which avoids duplicating the connectivity logic.

/// Evaluate a csg-cmp pair for plan construction.
void DPHypJoinOrderOptimizer::emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg)
{
    if (dphyp_unsupported_predicate)
        return;
    LOG_TEST(log, "DPhyp: emitCsgCmp({{ {} }}, {{ {} }})",
        fmt::join(left_csg, ","), fmt::join(right_csg, ","));
    tryJoin(left_csg, right_csg);
}

/// Recursively extend complement S2 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
void DPHypJoinOrderOptimizer::enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;

    LOG_TEST(log, "DPhyp: enumerateCmpRec(csg={{ {} }}, cmp={{ {} }}, excl={{ {} }})",
        fmt::join(csg, ","), fmt::join(complement, ","), fmt::join(exclusion, ","));

    BitSet complement_neighborhood = getNeighborhood(complement).andNot(exclusion);
    if (!complement_neighborhood)
        return;

    LOG_TEST(log, "DPhyp: enumerateCmpRec neighborhood={{ {} }}",
        fmt::join(complement_neighborhood, ","));

    /// First pass: emit pairs for every connected extension of the complement.
    forEachNonEmptySubset(complement_neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        BitSet extended_complement = complement | extension;
        if (dp_table.contains(extended_complement))
            emitCsgCmp(csg, extended_complement);
        return true;
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S2, X)).
    BitSet incremental_exclusion = exclusion | complement_neighborhood;
    forEachNonEmptySubset(complement_neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        enumerateCmpRec(csg, complement | extension, incremental_exclusion);
        return true;
    });
}

/// Generate all complement seeds for a given connected subgraph S1.
/// Seeds are single neighbor nodes, processed in descending index order. Each processed seed is
/// added to the exclusion passed to later seeds, so a complement spanning several neighbors is grown
/// from only one of them and each (S1, S2) pair is enumerated exactly once.
///
/// `exclusion` (paper: X) = S1 | B_min(S1), where B_min(S1) = {v : v < min(S1)}.
/// B_min excludes all relations ordered before the smallest relation in S1.
/// This is the key mechanism that prevents generating symmetric pairs:
/// the complement can only contain relations ordered after the CSG's minimum.
void DPHypJoinOrderOptimizer::emitCsg(const BitSet & csg)
{
    if (dphyp_unsupported_predicate)
        return;
    LOG_TEST(log, "DPhyp: emitCsg({{ {} }})", fmt::join(csg, ","));

    BitSet exclusion = csg | BitSet::allSet(*csg.begin());

    BitSet csg_neighborhood = getNeighborhood(csg).andNot(exclusion);
    if (!csg_neighborhood)
        return;

    LOG_TEST(log, "DPhyp: emitCsg neighborhood={{ {} }}, exclusion={{ {} }}",
        fmt::join(csg_neighborhood, ","), fmt::join(exclusion, ","));

    std::vector<size_t> neighbor_nodes;
    for (size_t n : csg_neighborhood)
        neighbor_nodes.push_back(n);

    /// Process seeds in descending index order, excluding each already-processed seed from the
    /// complements grown by later seeds. Without this, the same complement (e.g. {1,2}) would be
    /// reached from both the {2} seed and the {1} seed, enumerating the (S1, S2) pair twice.
    BitSet seed_exclusion = exclusion;
    for (auto it = neighbor_nodes.rbegin(); it != neighbor_nodes.rend(); ++it)
    {
        if (!continueEnumeration())
            return;
        BitSet single_node;
        single_node.set(*it);
        emitCsgCmp(csg, single_node);
        enumerateCmpRec(csg, single_node, seed_exclusion);
        seed_exclusion.set(*it);
    }
}

/// Recursively extend connected subgraph S1 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
/// For each connected extension found in dp_table, calls `emitCsg` to generate complements.
void DPHypJoinOrderOptimizer::enumerateCsgRec(const BitSet & csg, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;

    LOG_TEST(log, "DPhyp: enumerateCsgRec(csg={{ {} }}, excl={{ {} }})",
        fmt::join(csg, ","), fmt::join(exclusion, ","));

    BitSet neighborhood = getNeighborhood(csg).andNot(exclusion);
    if (!neighborhood)
        return;

    LOG_TEST(log, "DPhyp: enumerateCsgRec neighborhood={{ {} }}",
        fmt::join(neighborhood, ","));

    /// First pass: emit complements for every connected extension of S1.
    forEachNonEmptySubset(neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        BitSet extended_csg = csg | extension;
        if (dp_table.contains(extended_csg))
            emitCsg(extended_csg);
        return true;
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S1, X)).
    BitSet extended_exclusion = exclusion | neighborhood;
    forEachNonEmptySubset(neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        enumerateCsgRec(csg | extension, extended_exclusion);
        return true;
    });
}

std::shared_ptr<DPJoinEntry> DPHypJoinOrderOptimizer::solve()
{
    const size_t num_relations = query_graph.relation_stats.size();

    /// DPhyp's subset enumeration uses a 64-bit bitmask, so it cannot handle neighborhoods
    /// larger than 63 relations. Bail out gracefully so the fallback algorithm chain can continue.
    if (num_relations >= 64)
    {
        LOG_TRACE(log, "Too many relations ({}) for DPhyp, falling back", num_relations);
        return nullptr;
    }

    dphyp_unsupported_predicate = false;
    search_budget_exceeded = false;
    searched_plans = 0;

    /// Initialize dp_table with a leaf entry for each base relation.
    /// Also reset the per-edge selectivity cache so this run is independent of any
    /// earlier algorithm in the fallback chain.
    dp_table.clear();
    expression_selectivity.clear();
    for (size_t i = 0; i < num_relations; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats);
        dp_table[entry->relations] = entry;
    }

    buildHyperedges();

    LOG_TEST(log, "DPhyp: {} relations, {} hyperedges", num_relations, hyperedges.size());
    for (size_t e = 0; e < hyperedges.size(); ++e)
        LOG_TEST(log, "DPhyp: hyperedge {}: ({{ {} }}, {{ {} }})", e,
            fmt::join(hyperedges[e].left, ","), fmt::join(hyperedges[e].right, ","));

    /// Main DPhyp loop (paper: Solve, Sec 3.1).
    /// Seed with each single-relation CSG in descending index order.
    /// For each seed {v}, `emitCsg` finds complements (the other side of the join),
    /// and `enumerateCsgRec` grows {v} into larger connected subgraphs.
    /// The exclusion set B_v = {w : w < v} | {v} ensures each unordered (S1, S2) pair
    /// is considered exactly once (the side with the smaller min-index is always S1).
    BitSet exclusion = BitSet::allSet(num_relations);
    for (int i = static_cast<int>(num_relations) - 1; i >= 0; --i)
    {
        /// Once enumeration is aborted, the result is discarded below, so stop seeding.
        if (dphyp_unsupported_predicate || search_budget_exceeded)
            break;

        BitSet seed;
        seed.set(static_cast<size_t>(i));

        LOG_TEST(log, "DPhyp: === seed {} ===", i);
        emitCsg(seed);
        exclusion.set(i, false);
        enumerateCsgRec(seed, exclusion);
    }

    if (dphyp_unsupported_predicate || search_budget_exceeded)
    {
        LOG_TRACE(log, "DPhyp could not produce a plan ({}), falling back",
            dphyp_unsupported_predicate ? "unsupported predicate" : "search budget exceeded");
        return nullptr;
    }

    auto best = dp_table.find(BitSet::allSet(num_relations));
    if (best != dp_table.end())
        return best->second;

    /// DPhyp cannot produce a plan for disconnected graphs (no cross products).
    /// The caller's fallback chain (e.g. dphyp,greedy) handles this.
    LOG_TRACE(log, "Failed to find best plan using DPhyp algorithm");
    return nullptr;
}

}

DPJoinEntryPtr solveDPHypJoinOrder(
    QueryGraph & query_graph,
    UInt64 max_searched_plans,
    QueryStatusPtr query_status,
    std::function<bool()> interactive_cancel_callback)
{
    return DPHypJoinOrderOptimizer(
        query_graph,
        max_searched_plans,
        std::move(query_status),
        std::move(interactive_cancel_callback)).solve();
}

}
