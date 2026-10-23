#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/Optimizations/joinEnum.h>
#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>
#include <Processors/QueryPlan/Optimizations/conflictDetector.h>

#include <Interpreters/JoinOperator.h>
#include <Common/Exception.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

/// Whether the DP entries of a graph carry the real strictness of their joins. They do for an `ALL`
/// graph. Otherwise the strictness is stamped on the joins later and the entries read `All`, except
/// in a semi/anti graph flattened for the conflict detector, where each semi/anti join is an
/// operator of its own and its entry carries its strictness.
bool entriesCarryStrictness(const QueryGraph & query_graph)
{
    if (query_graph.join_strictness == JoinStrictness::All)
        return true;
    const bool is_semi_or_anti = query_graph.join_strictness == JoinStrictness::Semi
        || query_graph.join_strictness == JoinStrictness::Anti;
    return query_graph.semi_anti_flattened && is_semi_or_anti;
}

/// Whether a join may have its inputs swapped. Inner and cross joins commute, and an `ALL`
/// outer join is mirrored through `reverseJoinKind` (`Left` <-> `Right`), the same equivalence the
/// enumeration uses. Semi/anti joins keep their sides, and so do `ANY` joins: `ANY LEFT` keeps one
/// match per left row, `ANY RIGHT` one per right row.
bool canSwap(JoinKind kind, JoinStrictness strictness)
{
    if (isInner(kind) || isCrossOrComma(kind))
        return true;
    return strictness == JoinStrictness::All && (isLeft(kind) || isRight(kind) || isFull(kind));
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
        std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, const SelectivityEstimate & selectivity, JoinKind join_kind,
        JoinStrictness strictness = JoinStrictness::All) const;

    /// Native-mask counterparts used exclusively by the DPsub acceptor.
    void initDPsubScratch();
    std::optional<JoinKind> isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const;

    /// Conflict-detector (CD) variant: decide validity and the resulting (kind, strictness) using the
    /// per-operator descriptors in `dpsub_data.conflict_operators` (CD-A or CD-C), which support
    /// outer and semi/anti reordering. Returns nullopt to reject the split.
    std::optional<std::pair<JoinKind, JoinStrictness>> isValidJoinOrderMaskConflict(UInt32 left_mask, UInt32 right_mask) const;

    /// Dispatch used by the DPsub acceptor: routes to the per-operator conflict-detector check when
    /// a conflict detector (CD-A or CD-C) is enabled, otherwise to the per-relation
    /// `isValidJoinOrderMask` (with strictness fixed to All, its only supported case).
    std::optional<std::pair<JoinKind, JoinStrictness>> resolveJoinMask(UInt32 left_mask, UInt32 right_mask) const;

    /// True when an inner-join condition joins the two sides here: all its tables are present and it
    /// mentions both sides. Skips pinned conditions as those belong to an outer or semi/anti join's
    bool hasInnerEdgeAcross(UInt32 left_mask, UInt32 right_mask) const;

    /// The join kind as the query wrote it (`Left` or `Right`) of the semi/anti join step applied where
    /// `left_mask` and `right_mask` are joined, if there is one.
    std::optional<JoinKind> writtenSemiAntiKind(UInt32 left_mask, UInt32 right_mask, JoinStrictness strictness) const;

    /// True when a column equivalence ties the two sides together: some equivalence class has a
    /// member on each side. The mask counterpart of `QueryGraph::areTransitivelyConnected`.
    bool areTransitivelyConnectedMask(UInt32 left_mask, UInt32 right_mask) const;

    bool useConflictDetector() const
    {
        return query_graph.conflict_detector != JoinOrderConflictDetector::NONE;
    }
    ConflictDetector conflictDetectorKind() const
    {
        return query_graph.conflict_detector == JoinOrderConflictDetector::CD_C ? ConflictDetector::CDC : ConflictDetector::CDA;
    }

    const std::vector<JoinActionRef *> & collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask);
    SelectivityEstimate computeSelectivityMask(const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask);

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

        /// A member of an equivalence class, resolved once: its relation and the distinct values of
        /// its column, which do not change during the search.
        struct EquivMember
        {
            TUInt relation_bit = 0;
            std::optional<UInt64> ndv;
            UInt64 ndv_upper_bound = 0;
        };
        std::vector<std::vector<EquivMember>> equiv_classes; /// members of each distinct equivalence class
        std::vector<std::vector<TUInt>> rel_to_classes; /// relation id -> indices into equiv_classes
        std::vector<TUInt> class_relations;             /// per equivalence class: relations of its members

        std::vector<UInt64> class_visited;        /// generation stamp per equivalence class
        UInt64 equiv_generation = 0;              /// bumped on each computeSelectivityMask call
        std::vector<JoinActionRef *> applicable_scratch; /// reused output of collectJoinEdgesMask

        /// Whether the relations fall apart once cross products are set aside, computed in
        /// `initDPsubScratch` from the masks below. DPsub builds the full set out of connected
        /// pieces, so such a graph is one it cannot plan.
        bool disconnected_graph = false;

        /// Per-operator conflict descriptors (CD-A or CD-C), populated in `initDPsubScratch` only
        /// when a conflict detector is enabled. When non-empty, `isValidJoinOrderMaskConflict` uses
        /// these (per-operator required-set + conflict rules) instead of the per-relation
        /// `restriction_by_rel`, which lets non-commutative outer and semi/anti joins be reordered.
        std::vector<ConflictOperator> conflict_operators;

        /// Per edge, with a conflict detector: the index in `conflict_operators` of the join step
        /// whose ON clause it comes from, and, for an inner join's conjunct, its outstanding relations:
        /// the relations besides its own that must be present before it may be placed. See
        /// `initDPsubScratch`.
        std::vector<size_t> edge_operator;
        std::vector<TUInt> edge_outstanding_mask;
    };
    DPsubMaskData<UInt32> dpsub_data;

    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
};

std::optional<UInt64> DPSubJoinOrderOptimizer::estimateCardinality(
    std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, const SelectivityEstimate & selectivity, JoinKind join_kind,
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
        /// ON-clause predicates of an outer join are pinned to its null-supplying side; the pin
        /// becomes applicable exactly when that whole side is joined.
        if (auto pin_it = query_graph.outer_join_conditions.find(edge); pin_it != query_graph.outer_join_conditions.end())
        {
            dpsub_data.edge_pinned[i] = 1;
            dpsub_data.edge_pin_mask[i] = toMask(pin_it->second);
        }
    }

    /// Outer-join reordering constraints. Two mutually exclusive representations:
    ///  - default: per-relation ON-clause restrictions from `query_graph.join_kinds`, consumed by
    ///    `isValidJoinOrderMask`. Handles inner + outer joins only.
    ///  - conflict detector enabled: per-operator descriptors from CD-A or CD-C, consumed by
    ///    `isValidJoinOrderMaskConflict`. Handles inner/outer *and* semi/anti joins, with orientation.
    dpsub_data.restriction_by_rel.assign(num_relations, {});
    dpsub_data.conflict_operators.clear();
    dpsub_data.edge_operator.assign(n, 0);
    dpsub_data.edge_outstanding_mask.assign(n, 0);
    if (useConflictDetector())
    {
        std::vector<ConflictOpMask> ops;
        ops.reserve(query_graph.conflict_ops.size());
        for (const auto & op : query_graph.conflict_ops)
            ops.push_back(ConflictOpMask{toMask(op.left), toMask(op.right), toMask(op.nel), toMask(op.nr_rels), op.kind, op.strictness});

        dpsub_data.conflict_operators = computeConflictOperators(ops, conflictDetectorKind(), log);

        /// An inner join step does not fix the order of its inputs, and its ON clause is split into
        /// conjuncts, filters placed each at the lowest join where it can be evaluated. Neither may
        /// get ahead of a join nested under the step whose padded side it reads: below an outer,
        /// semi or anti join, a filter may only go onto the preserved side, or it would remove rows
        /// before that join pads or matches them. So such a nested join must have been applied first:
        /// the relations locating it are outstanding relations of the step, and of each of its
        /// conjuncts, which may only be applied once they are present.
        ///
        /// Exactly those, and not all relations of the nested join, keep a conjunct that reads two
        /// relations spanning the join it is placed at: the last of its relations and its outstanding
        /// relations to be joined is either one of its own, or one locating the nested join, which is
        /// then applied at that same join and takes the conjunct as a filter after it. More outstanding
        /// relations would place it higher, with both of its relations on one input, as a one-sided
        /// condition of an inner join that some join algorithms (`full_sorting_merge`) cannot execute.
        auto & operators = dpsub_data.conflict_operators;
        auto outstanding_relations = [&](UInt32 reads, size_t step) -> UInt32
        {
            const UInt32 step_relations = operators[step].relations;
            UInt32 outstanding = 0;
            for (const auto & nested : operators)
            {
                const bool is_nested = (nested.relations & ~step_relations) == 0 && nested.relations != step_relations;
                if (is_nested && (reads & nested.nullSupplying()))
                    outstanding |= nested.locatedBy();
            }
            return outstanding;
        };

        /// The inner steps first, from the ON clauses as written: `nel` is where `isAppliedAt` puts them.
        std::vector<UInt32> step_outstanding(operators.size(), 0);
        for (size_t step = 0; step < operators.size(); ++step)
            if (operators[step].freely_reorderable && !operators[step].degenerate)
                step_outstanding[step] = outstanding_relations(operators[step].nel, step);
        for (size_t step = 0; step < operators.size(); ++step)
            operators[step].nel |= step_outstanding[step];

        /// Where each condition is placed follows from the join step it comes from, see
        /// `collectJoinEdgesMask`: an outer, semi or anti join's condition goes where that join is
        /// applied, an inner join's conjunct as low as its relations and its outstanding relations allow.
        if (query_graph.edge_conflict_ops.size() != edges.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join graph has {} conditions, but join steps are recorded for {} of them",
                edges.size(), query_graph.edge_conflict_ops.size());

        for (size_t i = 0; i < edges.size(); ++i)
        {
            if (!edges[i])
                continue;
            const size_t op_index = query_graph.edge_conflict_ops[i];
            if (op_index >= operators.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join condition {} refers to join step {}, but there are only {}",
                    i, op_index, operators.size());
            dpsub_data.edge_operator[i] = op_index;
            if (dpsub_data.edge_pinned[i])
                continue;
            /// A constant conjunct reads no relation, but it filters the result of its own step: all
            /// relations of the step are outstanding for it, so it is neither applied below a join
            /// nested under the step nor to relations outside of it.
            const UInt32 sources = dpsub_data.edge_source_mask[i];
            dpsub_data.edge_outstanding_mask[i] = sources ? outstanding_relations(sources, op_index) : operators[op_index].relations;
        }
        LOG_TRACE(log, "DPsub: using {} conflict detector over {} captured join operators",
                  toString(query_graph.conflict_detector), dpsub_data.conflict_operators.size());
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
    dpsub_data.class_relations.clear();
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
        {
            auto & members = dpsub_data.equiv_classes.emplace_back();
            auto & relations = dpsub_data.class_relations.emplace_back(0);
            for (const auto & equiv_member : *class_ptr)
            {
                const auto & sources = equiv_member.getSourceRelations();
                auto relation = sources.getSingleBit();
                if (!relation || *relation >= num_relations)
                    continue;
                auto ndv = getColumnStats(query_graph, dp_table, sources, equiv_member.getColumnName());
                members.push_back({static_cast<UInt32>(1) << *relation, ndv.ndv, ndv.upper_bound});
                relations |= members.back().relation_bit;
            }
        }
        dpsub_data.rel_to_classes[*rel].push_back(it->second);
    }
    dpsub_data.class_visited.assign(dpsub_data.equiv_classes.size(), 0);
    dpsub_data.equiv_generation = 0;

    /// Connectivity: every predicate over two or more relations, every cross product (it links its two
    /// sides, see `QueryGraph::cross_product_links`), plus, with a conflict detector, one link per
    /// operator whose predicate does not span its two sides. What is still disconnected after that
    /// cannot be stitched together by DPsub.
    std::vector<size_t> component(num_relations);
    for (size_t i = 0; i < num_relations; ++i)
        component[i] = i;

    auto find = [&component](size_t x)
    {
        while (component[x] != x)
        {
            component[x] = component[component[x]];
            x = component[x];
        }
        return x;
    };
    auto unite = [&](UInt32 a_mask, UInt32 b_mask)
    {
        if (!a_mask || !b_mask)
            return;
        const size_t ra = find(static_cast<size_t>(std::countr_zero(a_mask)));
        const size_t rb = find(static_cast<size_t>(std::countr_zero(b_mask)));
        if (ra != rb)
            component[rb] = ra;
    };

    /// A condition over three or more tables (`a.x + b.x = c.x`) ties all of them together, just as a
    /// two-table one ties its pair.
    for (const UInt32 sources : dpsub_data.edge_source_mask)
    {
        if (std::popcount(sources) < 2)
            continue;
        const UInt32 lowest = sources & (~sources + 1);
        for (UInt32 rest = sources & ~lowest; rest; rest &= rest - 1)
            unite(lowest, rest & (~rest + 1));
    }

    /// An operator counts here only when degenerate - its predicate does not span its two sides. A
    /// spanning predicate is already linked by its edge above, and uniting the operator's whole
    /// subtrees instead would attach a relation that only a nested cross product holds on (`t1` in
    /// `t1 CROSS JOIN t2 JOIN t3 ON t2.k = t3.k`); that one is linked by its cross product below.
    for (const auto & op : dpsub_data.conflict_operators)
    {
        if (isCrossOrComma(op.kind) || !op.degenerate)
            continue;
        unite(op.left_relations, op.relations & ~op.left_relations);
    }

    for (const auto & [left, right] : query_graph.cross_product_links)
        unite(toMask(left), toMask(right));

    dpsub_data.disconnected_graph = false;
    for (size_t i = 1; i < num_relations; ++i)
    {
        if (find(i) != find(0))
        {
            dpsub_data.disconnected_graph = true;
            break;
        }
    }
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
    /// Decide whether this (left_mask, right_mask) split is a legal join and what kind/strictness it
    /// produces. Every operator applied across the split must pass: its required-left relations must
    /// sit in S1 and required-right in S2 (or mirrored), and each conflict rule T1 -> T2 must hold
    /// (if any table of T1 is joined, all of T2 must be too). Inner operators are checked too, since
    /// a nested operator's conflict with its parent is recorded on the parent, which may be inner.
    /// The single non-inner operator that crosses fixes the kind; two cannot share one node.
    const UInt32 combined = left_mask | right_mask;
    auto subset_of = [](const UInt32 a, const UInt32 b) { return (a & ~b) == 0; };

    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    bool have_non_inner = false;
    bool any_involved = false;
    /// Whether anything applied here actually ties the two sides together, as opposed to a cross
    /// product, which is applied across the split while joining the sides on nothing.
    bool have_connecting_op = false;

    for (const auto & op : dpsub_data.conflict_operators)
    {
        /// The same notion places the operator's ON condition, see `collectJoinEdgesMask`.
        if (!op.isAppliedAt(left_mask, right_mask))
            continue;
        any_involved = true;

        /// Conflict rules (CD-C; empty for CD-A). A rule T1 -> T2 is disobeyed when some table of T1
        /// is already in the joined set S but not all of T2 is -- that ordering would apply this
        /// operator before a conflicting operand is in place.
        for (const auto & rule : op.rules)
            if ((rule.t1 & combined) && (rule.t2 & ~combined))
                return std::nullopt;

        /// Required-set containment. `forward` keeps the operator's (left, right) inputs aligned
        /// with (left_mask, right_mask); `mirrored` swaps them: a valid equivalence for any of our
        /// operators via `reverseJoinKind` (Left<->Right, Full/Inner unchanged; for semi/anti it
        /// flips the preserved side). The two required sides are disjoint and, for a non-degenerate
        /// predicate, both non-empty, so at most one orientation can hold.
        bool forward = subset_of(op.required_left, left_mask) && subset_of(op.required_right, right_mask);
        bool mirrored = subset_of(op.required_left, right_mask) && subset_of(op.required_right, left_mask);

        /// A one-sided/cross-product operator has an empty required side, so the containment above
        /// cannot orient it. Require each whole input subtree on its own side instead: this orients
        /// it and rejects a fragmented split that pulls part of a subtree across the outer-join
        /// boundary (an invalid plan). This is a very rare condition.
        if (op.degenerate)
        {
            const UInt32 right_relations = op.relations & ~op.left_relations;
            forward = forward && subset_of(op.left_relations, left_mask) && subset_of(right_relations, right_mask);
            mirrored = mirrored && subset_of(op.left_relations, right_mask) && subset_of(right_relations, left_mask);
        }

        if (!forward && !mirrored)
            return std::nullopt;

        /// Inner joins impose no join kind and are commutative; only their gate matters. A cross
        /// product is equally reorderable, but unlike an inner join its kind is worth keeping, so
        /// note it and read it off below. It must not go through the kind-fixing path underneath:
        /// that rejects a second such operator at one node, and two cross products there are legal.
        if (op.freely_reorderable)
        {
            if (!isCrossOrComma(op.kind))
                have_connecting_op = true;
            continue;
        }

        /// A non-inner operator fixes the kind. Two of them at one node -> impossible order.
        if (have_non_inner)
            return std::nullopt;
        have_non_inner = true;

        /// A non-inner operator whose orientation is still ambiguous (both sides admissible) would
        /// have its preserved side guessed; fail closed instead (see `reverseJoinKind`, which flips
        /// Left<->Right / the semi/anti preserved side while `buildPhysicalPlan` keeps the child order).
        if (forward && mirrored)
            return std::nullopt;

        kind = forward ? op.kind : reverseJoinKind(op.kind);
        strictness = op.strictness;
    }

    /// No operator is applied across this split. A real predicate always maps to an operator, so the
    /// only legitimate no-operator split is a transitive inner join (two sides tied by a column
    /// equivalence, no direct predicate). Anything else reaching here is the synthetic cross-product
    /// connectivity with no operator spanning it, thus reject rather than invent an inner join.
    if (!any_involved
        && !hasInnerEdgeAcross(left_mask, right_mask)
        && !areTransitivelyConnectedMask(left_mask, right_mask))
        return std::nullopt;

    /// Only cross products were applied, and no operator fixed a kind, so the two sides are joined
    /// on nothing. Report that rather than the `Inner` this started as: an unconditioned join is a
    /// cross product, and `applyParallelReplicas` and `EXPLAIN` both go by the kind.
    if (kind == JoinKind::Inner && any_involved && !have_non_inner && !have_connecting_op
        && !hasInnerEdgeAcross(left_mask, right_mask)
        && !areTransitivelyConnectedMask(left_mask, right_mask))
        kind = JoinKind::Cross;

    return std::make_pair(kind, strictness);
}

bool DPSubJoinOrderOptimizer::hasInnerEdgeAcross(UInt32 left_mask, UInt32 right_mask) const
{
    const UInt32 combined = left_mask | right_mask;
    for (size_t i = 0; i < dpsub_data.edge_source_mask.size(); ++i)
    {
        if (dpsub_data.edge_pinned[i])
            continue;
        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if ((sources | dpsub_data.edge_outstanding_mask[i]) & ~combined)
            continue;
        if (!(sources & left_mask) || !(sources & right_mask))
            continue;
        /// A conjunct joining the two sides applies part of its join step here, so the step's conflict
        /// rules must hold for the joined relations, or the conjunct does not join them.
        bool rule_violated = false;
        for (const auto & rule : dpsub_data.conflict_operators[dpsub_data.edge_operator[i]].rules)
            if ((rule.t1 & combined) && (rule.t2 & ~combined))
                rule_violated = true;
        if (!rule_violated)
            return true;
    }
    return false;
}

std::optional<JoinKind> DPSubJoinOrderOptimizer::writtenSemiAntiKind(UInt32 left_mask, UInt32 right_mask, JoinStrictness strictness) const
{
    const auto & operators = dpsub_data.conflict_operators;
    for (size_t i = 0; i < operators.size(); ++i)
    {
        if (operators[i].strictness == strictness && operators[i].isAppliedAt(left_mask, right_mask))
            return query_graph.conflict_ops[i].kind;
    }
    return std::nullopt;
}

bool DPSubJoinOrderOptimizer::areTransitivelyConnectedMask(UInt32 left_mask, UInt32 right_mask) const
{
    for (const UInt32 relations : dpsub_data.class_relations)
    {
        if ((relations & left_mask) && (relations & right_mask))
            return true;
    }
    return false;
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
    const bool by_operator = useConflictDetector();
    auto & edges = query_graph.edges;

    for (size_t i = 0; i < edges.size(); ++i)
    {
        auto & edge = edges[i];
        if (!edge)
            continue;

        /// With a conflict detector, the ON condition of an outer, semi or anti join is placed exactly
        /// where that join is applied, the join whose kind it fixes (see `isValidJoinOrderMaskConflict`),
        /// and so it stays in that join's ON clause, whatever the search does around it.
        if (by_operator && dpsub_data.edge_pinned[i])
        {
            if (dpsub_data.conflict_operators[dpsub_data.edge_operator[i]].isAppliedAt(left_mask, right_mask))
                out.push_back(&edge);
            continue;
        }

        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if (sources & ~joined) /// edge sources not proper subset of joined relations
            continue;
        if (dpsub_data.edge_pinned[i] && (dpsub_data.edge_pin_mask[i] & ~joined))
            continue;

        /// For an outer join the `pin` mask encodes relations that must be present for the predicate
        /// to be applicable; for an inner join it is just the predicate's source relations (pin empty).
        /// For a single-table conjunct of an outer join's ON
        /// clause (e.g. `t2.value = 'x'` in `... LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'x'`),
        /// `sources` is only `{t2}` but the pin is `{t3}`: the predicate belongs to the ON condition of
        /// the join that brings in `t3`, not to `t2` as a base-table filter. Placing it by `sources`
        /// alone would push it below (or drop it from) that join, silently changing outer-join
        /// semantics, since `t2` may already have been NULL-extended (or default-filled) by an
        /// earlier join. Without a conflict detector, a null-supplying side is a single relation, which
        /// makes this placement exact. With one, the pin is unused, but an inner join's conjunct with
        /// outstanding relations is placed the same way (see `initDPsubScratch`).
        const UInt32 pin = dpsub_data.edge_pinned[i] ? dpsub_data.edge_pin_mask[i] : dpsub_data.edge_outstanding_mask[i];
        if (pin & ~joined)
            continue;
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

SelectivityEstimate DPSubJoinOrderOptimizer::computeSelectivityMask(
    const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask)
{
    auto estimate = DB::computeSelectivity(query_graph, dp_table, expression_selectivity, edges);

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

            UInt64 max_ndv = 0;
            UInt64 max_ndv_upper_bound = 0;
            bool has_left = false;
            bool has_right = false;
            for (const auto & equiv_member : dpsub_data.equiv_classes[class_idx])
            {
                if (left_mask & equiv_member.relation_bit)
                    has_left = true;
                else if (right_mask & equiv_member.relation_bit)
                    has_right = true;
                else
                    continue;
                max_ndv = std::max(max_ndv, equiv_member.ndv.value_or(0));
                max_ndv_upper_bound = std::max(max_ndv_upper_bound, equiv_member.ndv_upper_bound);
            }
            if (has_left && has_right)
                applyEquiKeyDistinctValues(estimate, max_ndv, max_ndv_upper_bound);
        }
    }

    return estimate;
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

    /// A semi/anti join leaves the search in whichever orientation the enumeration reached first:
    /// mirroring it changes neither its cost nor its result. Emit it the way the query wrote it, so the
    /// join algorithms that execute the query as written also execute the reordered plan (partial merge
    /// join runs `LEFT SEMI` but not `RIGHT SEMI`). Which reorderings are valid is decided by the conflict
    /// detector alone; the orientation is a physical detail that must not make a query fail.
    if (entry.strictness == JoinStrictness::Semi || entry.strictness == JoinStrictness::Anti)
    {
        if (auto written_kind = writtenSemiAntiKind(entry.left, entry.right, entry.strictness);
            written_kind && *written_kind != join_operator.kind)
        {
            left.swap(right);
            join_operator.kind = reverseJoinKind(join_operator.kind);
        }
    }

    /// The search costs a join the same whichever way round its inputs go, so `entry.right` is just
    /// the side the enumeration reached first. The right side builds the hash table, so give that
    /// role to the smaller estimate.
    ///
    /// Leave the sides alone when the query pins `query_plan_join_swap_table`, and in a graph whose
    /// entries do not carry the real strictness: the swap in `optimizeJoin` decides those later.
    const bool can_swap = entriesCarryStrictness(query_graph) && canSwap(entry.kind, entry.strictness);
    if (!query_graph.join_swap_table && can_swap)
    {
        const auto & left_rows = left->estimated_rows;
        const auto & right_rows = right->estimated_rows;
        /// On equal estimates, or when an input has none (e.g. a `UNION ALL` subquery), put the input
        /// with more relations on the left: the enumeration reaches a lone relation against the rest
        /// first, and would otherwise build the hash table from a whole subtree. Two single
        /// relations without an estimate keep their sides.
        const bool fewer_relations_left = std::popcount(entry.left) < std::popcount(entry.right);
        const bool swap_sides = (left_rows && right_rows)
            ? (*left_rows < *right_rows || (*left_rows == *right_rows && fewer_relations_left))
            : fewer_relations_left;

        if (swap_sides)
        {
            left.swap(right);
            join_operator.kind = reverseJoinKind(join_operator.kind);
        }
    }

    return std::make_shared<DPJoinEntry>(left, right, entry.cost, entry.sel, entry.estimated_rows, std::move(join_operator));
}

std::shared_ptr<DPJoinEntry> DPSubJoinOrderOptimizer::solve()
{
    /// DPsub uses the generic memo only for composite-expression statistics. Clear any
    /// partial state left by an earlier algorithm in the fallback chain so this attempt
    /// has the same cost model as a standalone DPsub run.
    dp_table.clear();
    expression_selectivity.clear();

    const size_t n = query_graph.relation_stats.size();
    using Bitvector = UInt32; // choose UInt64 or even UInt128 for larger sets

    /// Check the table count up front rather than stopping midway through: a half-finished search
    /// throws away the time it already spent and still ends up handing the query to greedy.
    static_assert(DPSUB_MAX_RELATIONS < std::numeric_limits<Bitvector>::digits);

    if (n > DPSUB_MAX_RELATIONS)
    {
        LOG_TRACE(log,
            "Number of relations {} exceeds the DPsub threshold {}, skipping DPsub optimization and falling back to the next algorithm",
            n, DPSUB_MAX_RELATIONS);
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

    /// Turn down a disconnected graph before enumerating anything: DPsub cannot stitch its
    /// components, so the next algorithm in the chain plans the query instead. Cross products do not
    /// disconnect a graph: each one links its two sides (see `QueryGraph::cross_product_links`),
    /// whatever the conflict detector, and the acceptor reports such a join as `Cross`. What is left
    /// is a join whose condition does not span its sides without a conflict detector to link it.
    /// A graph only DPsub may plan is never turned down.
    if (dpsub_data.disconnected_graph && !query_graph.requires_conflict_detector)
    {
        LOG_TRACE(log, "Join graph is disconnected apart from cross products, leaving it to the next algorithm");
        return nullptr;
    }

    Checker checker(n, *this);
    Enumerator enumerator(n, log);
    enumerator.enumerate(checker, query_graph);

    const Bitvector full_set = (static_cast<Bitvector>(1) << n) - 1;
    const auto & dptable = checker.getDPTable();

    /// The full set is assembled only if the join graph is connected. When it is not e.g.
    /// cross products, or predicates that reference a single relation or a constant and thus
    /// create no binary edge (`... LEFT JOIN t ON t.x = 5`): DPsub cannot stitch the
    /// disconnected components, so the full-set entry is missing (or was never given a join)
    const bool full_built = dptable.isConnected(full_set)
                            && (dptable[full_set].left != 0 || dptable[full_set].right != 0);
    if (!full_built)
    {
        LOG_TRACE(log, "DPsub: join graph is disconnected");
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
