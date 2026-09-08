#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/Optimizations/joinEnum.h>
#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>
#include <Processors/QueryPlan/Optimizations/conflictDetector.h>

#include <Interpreters/JoinOperator.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace
{

/// Pack a `BitSet` (boost::dynamic_bitset) of relation ids into a native 32-bit mask.
/// Used by the DPsub hot path, where the relation count is guaranteed below 32 so every
/// set bit fits, allowing the per-CCP work to run on native integers instead of allocating
/// a `dynamic_bitset` for every plan it considers.
UInt32 toMask(const BitSet & bits)
{
    UInt32 mask = 0;
    for (auto bit : bits)
    {
        chassert(bit < std::numeric_limits<UInt32>::digits);
        mask |= (static_cast<UInt32>(1) << bit);
    }
    return mask;
}

class DPSubJoinOrderOptimizer
{
public:
    explicit DPSubJoinOrderOptimizer(QueryGraph & query_graph_)
        : query_graph(query_graph_)
    {
    }

    DPJoinEntryPtr solve();

private:
    template <typename DPTable, std::unsigned_integral TUInt>
    DPJoinEntryPtr buildPhysicalPlan(const DPTable & dptable, const TUInt & S) const;

    template <class TDPTable, class TOptimizer>
    friend class DB::EnumeratorCheckerWithCosts;

    std::optional<UInt64> estimateCardinality(
        std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, double selectivity, JoinKind join_kind,
        JoinStrictness strictness = JoinStrictness::All) const;

    /// Native-mask counterparts used exclusively by the DPsub acceptor.
    void initDPsubScratch();
    std::optional<JoinKind> isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const;

    /// Conflict-detector variant: decide validity and the resulting (kind, strictness) using the
    /// per-operator descriptors in `dpsub_data.conflict_operators` (CD-A or CD-C), which support
    /// outer and semi/anti reordering. Returns nullopt to reject the split.
    std::optional<std::pair<JoinKind, JoinStrictness>> isValidJoinOrderMaskConflict(UInt32 left_mask, UInt32 right_mask) const;

    /// Dispatch used by the DPsub acceptor: routes to the per-operator conflict-detector check when
    /// a conflict detector (CD-A or CD-C) is enabled, otherwise to the per-relation
    /// `isValidJoinOrderMask` (with strictness fixed to All, its only supported case).
    std::optional<std::pair<JoinKind, JoinStrictness>> resolveJoinMask(UInt32 left_mask, UInt32 right_mask) const;

    bool useConflictDetector() const
    {
        return query_graph.use_cd_a_conflict_detector || query_graph.use_cd_c_conflict_detector;
    }
    ConflictDetector conflictDetectorKind() const
    {
        return query_graph.use_cd_c_conflict_detector ? ConflictDetector::CDC : ConflictDetector::CDA;
    }

    const std::vector<JoinActionRef *> & collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask);
    double computeSelectivityMask(const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask);

    QueryGraph & query_graph;
    SelectivityCache expression_selectivity;
    PlanMemo dp_table;

    /** Precomputed native-mask view of the query graph for the DPsub hot path. Populated once per
    * `solve` run by `initDPsubScratch`. Each `BitSet` is packed into a `UInt` type, the outer-join
    * restrictions are indexed by relation, and column-equivalence classes are indexed by relation so
    * `computeSelectivityMask` only visits the classes incident to the left side instead of rescanning
    * the whole equivalence map. The trailing fields are reusable scratch (no per-CCP allocation)
    */
    template <std::unsigned_integral TUInt>
    struct DPsubMaskData
    {
        using EquivClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;

        std::vector<TUInt> edge_source_mask;   /// per edge: source relations
        std::vector<TUInt> edge_pin_mask;      /// per edge: relations that must all be present (if pinned)
        std::vector<char> edge_pinned;         /// per edge: whether a pin applies

        struct Restriction
        {
            TUInt required = 0;
            JoinKind kind = JoinKind::Inner;
            bool present = false;
        };
        std::vector<Restriction> restriction_by_rel; /// indexed by relation id

        std::vector<EquivClassPtr> equiv_classes;        /// distinct equivalence classes
        std::vector<std::vector<TUInt>> rel_to_classes; /// relation id -> indices into equiv_classes

        std::vector<UInt64> class_visited;        /// generation stamp per equivalence class
        UInt64 equiv_generation = 0;              /// bumped on each computeSelectivityMask call
        std::vector<JoinActionRef *> applicable_scratch; /// reused output of collectJoinEdgesMask

        /// Per-operator conflict descriptors (CD-A or CD-C), populated in `initDPsubScratch` only
        /// when a conflict detector is enabled. When non-empty, `isValidJoinOrderMaskConflict` uses
        /// these (per-operator required-set + conflict rules) instead of the per-relation
        /// `restriction_by_rel`, which lets non-commutative outer and semi/anti joins be reordered.
        std::vector<ConflictOperator> conflict_operators;
    };
    DPsubMaskData<UInt32> dpsub_data;

    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
};

std::optional<UInt64> DPSubJoinOrderOptimizer::estimateCardinality(
    std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, double selectivity, JoinKind join_kind,
    JoinStrictness strictness) const
{
    return estimateJoinCardinality(left_rows, right_rows, selectivity, join_kind, strictness);
}

void DPSubJoinOrderOptimizer::initDPsubScratch()
{
    using EquivClass = EquivalenceClasses<JoinActionRef>::Class;

    const size_t num_relations = query_graph.relation_stats.size();
    const auto & edges = query_graph.edges;

    /// Edges: precompute the source-relation mask and any pin mask once per edge
    auto n = edges.size();
    dpsub_data.edge_source_mask.assign(n, 0);
    dpsub_data.edge_pin_mask.assign(n, 0);
    dpsub_data.edge_pinned.assign(n, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const auto & edge = edges[i];
        if (!edge)
            continue;
        dpsub_data.edge_source_mask[i] = toMask(edge.getSourceRelations());
        /// ON-clause predicates of an outer join are pinned to the single null-supplying
        /// relation; the pin becomes applicable exactly when that relation is joined.
        if (auto pin_it = query_graph.outer_join_conditions.find(edge); pin_it != query_graph.outer_join_conditions.end())
        {
            dpsub_data.edge_pinned[i] = 1;
            dpsub_data.edge_pin_mask[i] = static_cast<UInt32>(1) << pin_it->second;
        }
    }

    /// Outer-join reordering constraints. Two mutually exclusive representations:
    ///  - default: per-relation ON-clause restrictions from `query_graph.join_kinds`, consumed by
    ///    `isValidJoinOrderMask`. Handles inner + outer joins only.
    ///  - conflict detector enabled: per-operator descriptors from CD-A or CD-C, consumed by
    ///    `isValidJoinOrderMaskConflict`. Handles inner/outer *and* semi/anti joins, with orientation.
    dpsub_data.restriction_by_rel.assign(num_relations, {});
    dpsub_data.conflict_operators.clear();
    if (useConflictDetector())
    {
        std::vector<ConflictOpMask> ops;
        ops.reserve(query_graph.conflict_ops.size());
        for (const auto & op : query_graph.conflict_ops)
            ops.push_back(ConflictOpMask{toMask(op.left), toMask(op.right), toMask(op.nel), toMask(op.nr_rels), op.kind, op.strictness});

        dpsub_data.conflict_operators = computeConflictOperators(ops, conflictDetectorKind(), log);
        LOG_TRACE(log, "DPsub: using {} conflict detector over {} captured join operators",
                  query_graph.use_cd_c_conflict_detector ? "CD-C" : "CD-A", dpsub_data.conflict_operators.size());
    }
    else
    {
        for (const auto & [rel, restriction] : query_graph.join_kinds)
        {
            if (rel >= num_relations)
                continue;
            auto & native = dpsub_data.restriction_by_rel[rel];
            native.required = toMask(restriction.first);
            native.kind = restriction.second;
            native.present = true;
        }
    }

    /// Column-equivalence classes: build a per-relation incidence list so selectivity only
    /// visits the classes touching the left side instead of rescanning the whole member map
    dpsub_data.equiv_classes.clear();
    dpsub_data.rel_to_classes.assign(num_relations, {});
    std::unordered_map<const EquivClass *, UInt32> class_index;
    for (const auto & [member, class_ptr] : query_graph.column_equivalences.getMemberToClassMap())
    {
        if (!class_ptr)
            continue;
        auto rel = member.getSourceRelations().getSingleBit();
        if (!rel || *rel >= num_relations)
            continue;

        auto [it, inserted] = class_index.try_emplace(class_ptr.get(), static_cast<UInt32>(dpsub_data.equiv_classes.size()));
        if (inserted)
            dpsub_data.equiv_classes.push_back(class_ptr);
        dpsub_data.rel_to_classes[*rel].push_back(it->second);
    }
    dpsub_data.class_visited.assign(dpsub_data.equiv_classes.size(), 0);
    dpsub_data.equiv_generation = 0;
}

std::optional<JoinKind> DPSubJoinOrderOptimizer::isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const
{
    auto check = [&](UInt32 lhs, UInt32 rhs) -> std::optional<JoinKind>
    {
        if (std::popcount(lhs) == 1)
        {
            const auto & restriction = dpsub_data.restriction_by_rel[std::countr_zero(lhs)];
            if (restriction.present)
            {
                /// If there are any bits set in `restriction.required`
                /// that are not set in `rhs`, the bitwise AND (&) results in a non-zero value
                if (restriction.required & ~rhs)
                    return {};
                return restriction.kind;
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
        left_join_type = isLeftOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (auto res = check(right_mask, left_mask))
        right_join_type = isRightOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (left_join_type == JoinKind::Inner)
        return right_join_type;
    if (right_join_type == JoinKind::Inner)
        return left_join_type;
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflicting outer-join constraints, the order is not possible
    return {};
}

std::optional<std::pair<JoinKind, JoinStrictness>>
DPSubJoinOrderOptimizer::isValidJoinOrderMaskConflict(UInt32 left_mask, UInt32 right_mask) const
{
    /// Unified `applicable` for CD-A (Section 5.2) and CD-C (Section 5.4). The enumerator proposes
    /// each connected, non-overlapping (left_mask, right_mask) split once. We look at every operator
    /// whose ON predicate is applied across this split and require it to be `applicable`:
    ///   required_left(op) subseteq S1  AND  required_right(op) subseteq S2  (forward, or mirrored),
    ///   AND every conflict rule T1 -> T2 obeyed: T1 met by S implies T2 subseteq S.
    /// For CD-A the required set is the widened TES and there are no rules; for CD-C it is the SES
    /// plus conflict rules. Every crossing operator -- inner joins included -- must pass, because a
    /// conflict between a nested operator and its parent is recorded in the *parent's* descriptor,
    /// and that parent may itself be an inner join. The single non-inner operator that crosses (if
    /// any) fixes the resulting join kind/strictness; two of them cannot share one binary node, so
    /// the split is rejected. If none crosses, the step is a plain inner join.
    const UInt32 combined = left_mask | right_mask;
    auto subset_of = [](const UInt32 a, const UInt32 b) { return (a & ~b) == 0; };

    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    bool have_non_inner = false;

    for (const auto & op : dpsub_data.conflict_operators)
    {
        /// The operator is applied at this boundary when its ON predicate (NEL) spans the split.
        /// Using the operator's *relation set* to detect straddling is wrong: an ancestor's
        /// relation set is a superset of this subset, and an operator already applied inside a
        /// child no longer has a crossing predicate. The relation-set straddle is a fallback only
        /// for degenerate predicate-less operators (empty NEL, e.g. an ON-TRUE join).
        const bool within = subset_of(op.relations, combined);
        const bool nel_crosses = (op.nel & left_mask) && (op.nel & right_mask);
        const bool rel_straddles = (op.relations & left_mask) && (op.relations & right_mask);
        const bool involved = nel_crosses || (op.nel == 0 && rel_straddles && within);
        if (!involved)
            continue;

        /// Conflict rules (CD-C; empty for CD-A). A rule T1 -> T2 is disobeyed when some table of T1
        /// is already in the joined set S but not all of T2 is -- that ordering would apply this
        /// operator before a conflicting operand is in place.
        for (const auto & rule : op.rules)
            if ((rule.t1 & combined) && (rule.t2 & ~combined))
                return std::nullopt;

        /// Required-set containment. `forward` keeps the operator's (left, right) inputs aligned
        /// with (left_mask, right_mask); `mirrored` swaps them -- a valid equivalence for any of our
        /// operators via `reverseJoinKind` (Left<->Right, Full/Inner unchanged; for semi/anti it
        /// flips the preserved side). The two required sides are disjoint and, for a non-degenerate
        /// predicate, both non-empty, so at most one orientation can hold.
        bool forward = subset_of(op.required_left, left_mask) && subset_of(op.required_right, right_mask);
        bool mirrored = subset_of(op.required_left, right_mask) && subset_of(op.required_right, left_mask);
        if (!forward && !mirrored)
            return std::nullopt;

        /// Inner joins impose no join kind and are commutative; only their gate matters.
        if (op.freely_reorderable)
            continue;

        /// A non-inner operator fixes the kind. Two of them at one node -> impossible order.
        if (have_non_inner)
            return std::nullopt;
        have_non_inner = true;

        /// For a non-degenerate predicate the two required sides are non-empty and disjoint, so
        /// exactly one orientation holds. A degenerate (predicate-less, e.g. ON TRUE) operator has
        /// empty required sets, so both orientations pass and the required-set test cannot tell which
        /// side is preserved. Break the tie by the operator's original subtree placement: its
        /// (left-canonical) preserved subtree `left_relations` must land on the preserving side.
        /// Fail closed if it is split across both sides: do not guess and risk flipping the
        /// preserved side (see `reverseJoinKind`, which flips Left<->Right / the semi/anti preserved
        /// side while `buildPhysicalPlan` keeps the child order).
        if (forward && mirrored)
        {
            if (subset_of(op.left_relations, right_mask))
                forward = false;
            else if (!subset_of(op.left_relations, left_mask))
                return std::nullopt;
        }

        kind = forward ? op.kind : reverseJoinKind(op.kind);
        strictness = op.strictness;
    }

    return std::make_pair(kind, strictness);
}

std::optional<std::pair<JoinKind, JoinStrictness>>
DPSubJoinOrderOptimizer::resolveJoinMask(UInt32 left_mask, UInt32 right_mask) const
{
    if (useConflictDetector())
        return isValidJoinOrderMaskConflict(left_mask, right_mask);

    if (auto kind = isValidJoinOrderMask(left_mask, right_mask))
        return std::make_pair(*kind, JoinStrictness::All);
    return std::nullopt;
}

const std::vector<JoinActionRef *> & DPSubJoinOrderOptimizer::collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask)
{
    auto & out = dpsub_data.applicable_scratch;
    out.clear();

    const UInt32 joined = left_mask | right_mask;
    const bool two_relations = std::popcount(joined) == 2;
    auto & edges = query_graph.edges;

    for (size_t i = 0; i < edges.size(); ++i)
    {
        auto & edge = edges[i];
        if (!edge)
            continue;

        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if (sources & ~joined) /// edge sources not proper subset of joined relations
            continue;
        if (dpsub_data.edge_pinned[i] && (dpsub_data.edge_pin_mask[i] & ~joined))
            continue;

        /// Works much like Extended Eligibility List (EEL) in case of outerjoins:
        /// encoding relations that must be present for the predicate to be applicable (in `pin` mask)
        /// For innerjoins its just the sources of the predicate, i.e., NEL, here pin is empty.
        /// For a single-table conjunct of an outer join's ON
        /// clause (e.g. `t2.value = 'x'` in `... LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'x'`),
        /// `sources` is only `{t2}` but the pin is `{t3}`: the predicate belongs to the ON condition of
        /// the join that brings in `t3`, not to `t2` as a base-table filter. Placing it by `sources`
        /// alone would push it below (or drop it from) that join, silently changing outer-join
        /// semantics, since `t2` may already have been NULL-extended (or default-filled) by an
        /// earlier join.
        const UInt32 pin = dpsub_data.edge_pinned[i] ? dpsub_data.edge_pin_mask[i] : 0;
        const UInt32 applicable = sources | pin;

        if (std::popcount(applicable) <= 1)
        {
            /// Base-relation filter or constant predicate (the edge references at most one relation).
            const bool relation_introduced = applicable != 0 && (left_mask == applicable || right_mask == applicable);
            const bool constant_at_earliest_join = applicable == 0 && two_relations;
            if (relation_introduced || constant_at_earliest_join)
                out.push_back(&edge);
        }
        else if ((applicable & ~left_mask) && (applicable & ~right_mask))
        {
            /// The predicate spans the split (a connecting equi-predicate, or a single-table ON-clause
            /// conjunct pinned to the opposite side): neither side alone contains all the relations it
            /// needs. This join is the lowest one that makes it applicable, so attach it here — into the
            /// correct join's ON condition.
            out.push_back(&edge);
        }
    }
    return out;
}

double DPSubJoinOrderOptimizer::computeSelectivityMask(
    const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask)
{
    double selectivity = DB::computeSelectivity(query_graph, dp_table, expression_selectivity, edges);

    /// Account for transitively-equivalent columns spanning both sides, visiting only the classes
    /// incident to the left relations. A generation stamp deduplicates classes without allocating
    const UInt64 generation = ++dpsub_data.equiv_generation;

    for (UInt32 remaining = left_mask; remaining;)
    {
        const UInt32 rel = std::countr_zero(remaining);
        remaining &= remaining - 1;

        for (UInt32 class_idx : dpsub_data.rel_to_classes[rel])
        {
            if (dpsub_data.class_visited[class_idx] == generation)
                continue;
            dpsub_data.class_visited[class_idx] = generation;

            size_t max_ndv = 0;
            bool has_left = false;
            bool has_right = false;
            for (const auto & equiv_member : *dpsub_data.equiv_classes[class_idx])
            {
                auto relation = equiv_member.getSourceRelations().getSingleBit();
                if (!relation)
                    continue;
                const UInt32 relation_bit = static_cast<UInt32>(1) << *relation;
                if (left_mask & relation_bit)
                {
                    has_left = true;
                    max_ndv = std::max(max_ndv, getColumnStats(query_graph, dp_table, equiv_member.getSourceRelations(), equiv_member.getColumnName()));
                }
                else if (right_mask & relation_bit)
                {
                    has_right = true;
                    max_ndv = std::max(max_ndv, getColumnStats(query_graph, dp_table, equiv_member.getSourceRelations(), equiv_member.getColumnName()));
                }
            }
            if (has_left && has_right && max_ndv > 0)
                selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
        }
    }

    return selectivity;
}

template <typename DPTable, std::unsigned_integral TUInt>
std::shared_ptr<DPJoinEntry> DPSubJoinOrderOptimizer::buildPhysicalPlan(const DPTable & dptable, const TUInt & S) const
{
    auto& entry = dptable[S];
    if (!entry.left && !entry.right)
        return std::make_shared<DPJoinEntry>(std::countr_zero(S), entry.estimated_rows, entry.column_stats);

    /// `entry.strictness` is All for every DP entry except semi/anti joins admitted by the
    /// conflict detector (CD-A/CD-C), which must keep their strictness in the reordered tree.
    JoinOperator join_operator(entry.kind, entry.strictness, JoinLocality::Unspecified);
    /// A filter predicate applied at an outer join step must not go to the ON clause, where it
    /// would affect matching instead of filtering and let non-matching rows of the preserved side
    /// survive NULL-extended. Apply it after the join instead (see `solveGreedy`).
    /// Semi/anti joins keep their ON predicates in the ON clause (they are in
    /// `outer_join_conditions`), so this only diverts genuine post-join filters.
    bool is_inner_step = isInner(entry.kind) || isCrossOrComma(entry.kind);
    for (const auto * e : entry.edges)
    {
        if (is_inner_step || query_graph.outer_join_conditions.contains(*e))
            join_operator.expression.push_back(*e);
        else
            join_operator.residual_filter.push_back(*e);
    }

    auto left = buildPhysicalPlan(dptable, entry.left);
    auto right = buildPhysicalPlan(dptable, entry.right);
    return std::make_shared<DPJoinEntry>(left, right, entry.cost, entry.estimated_rows, std::move(join_operator));
}

/** Implements the `Dpsub` bottom-up dynamic programming algorithm for optimal bushy join tree generation.
* This algorithm constructs optimal join trees by iterating over subsets of the relations in an ascending order
* based on an integer bitmask (from 1 to 2^n - 2). This ordering ensures that for any relation subset S,
* the best plans for all its proper sub-plans (subsets S1 ⊂ S) have already been computed, thereby adhering to
* Bellman's optimality principle.
* This methodical evaluation of all connected subsets results in the creation of the best plan for each.
* The final answer is the optimal plan for the complete set of relations.
* For more detailed information, see "Building Query Compilers":
* (https://pi3.informatik.uni-mannheim.de/~moer/querycompiler.pdf)
*/
std::shared_ptr<DPJoinEntry> DPSubJoinOrderOptimizer::solve()
{
    /// DPsub uses the generic memo only for composite-expression statistics. Clear any
    /// partial state left by an earlier algorithm in the fallback chain so this attempt
    /// has the same cost model as a standalone DPsub run.
    dp_table.clear();
    expression_selectivity.clear();

    const size_t n = query_graph.relation_stats.size();
    using Bitvector = UInt32; // choose UInt64 or even UInt128 for larger sets
    // A budget cap on nr. of connected components considered by DPsub to avoid excessive optimization time on large join graphs.
    // This budget cap is obtained from empirical testing using different queries and join graphs.
    static constexpr UInt32 max_nr_ccps = 50'000;

    if (n >= std::numeric_limits<Bitvector>::digits)
    {
        LOG_TRACE(log,
            "Number of relations {} exceeds the DP threshold {}, skipping DP optimization invoking greedy algorithm",
            n, std::numeric_limits<Bitvector>::digits);
        return nullptr;
    }

    struct DPEntry
    {
        Bitvector neighbor{0};
        Bitvector left{0};
        Bitvector right{0};
        std::optional<UInt64> estimated_rows = {};
        std::unordered_map<String, ColumnStats> column_stats = {};
        double cost{.0};
        double sel{.0};
        JoinKind kind{JoinKind::Inner};
        JoinStrictness strictness{JoinStrictness::All};
        std::vector<JoinActionRef*> edges; // needed for physical plan generation
    };
    using DPTable = DPTable<DPEntry, Bitvector>;
    using Checker = EnumeratorCheckerWithCosts<DPTable, DPSubJoinOrderOptimizer>;
    using Enumerator = EnumCcpSub<Checker, DPTable, QueryGraph>;

    /// Precompute the native-mask view of the query graph so the acceptor's per-CCP work
    /// (validity, edge collection, selectivity) runs without allocating a `BitSet` each time
    /// That is, we don't have to convert Bitvector -> BitSet -> Bitvector for every subset S
    /// and its subcomponents S1, S2
    initDPsubScratch();

    Checker checker(n, *this);
    Enumerator enumerator(n, max_nr_ccps, log);
    enumerator.enumerate(checker, query_graph);

    const Bitvector full_set = (static_cast<Bitvector>(1) << n) - 1;
    const auto & dptable = checker.getDPTable();

    /// a. If the join graph is too complex we break early
    /// b. The full set is assembled only if the join graph is connected. When it is not e.g.
    /// cross products, or predicates that reference a single relation or a constant and thus
    /// create no binary edge (`... LEFT JOIN t ON t.x = 5`): DPsub cannot stitch the
    /// disconnected components, so the full-set entry is missing (or was never given a join)
    const bool full_built = dptable.isConnected(full_set)
                            && (dptable[full_set].left != 0 || dptable[full_set].right != 0);
    if (!full_built)
    {
        LOG_TRACE(log, "DPsub: join graph is either too complex or disconnected!");
        return nullptr;
    }

    return buildPhysicalPlan(dptable, full_set);
}

}

DPJoinEntryPtr solveDPSubJoinOrder(QueryGraph & query_graph)
{
    return DPSubJoinOrderOptimizer(query_graph).solve();
}

}
