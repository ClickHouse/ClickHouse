#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/Optimizations/joinEnum.h>
#include <Common/CurrentThread.h>

#include <algorithm>
#include <bit>
#include <functional>
#include <limits>
#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/safe_cast.h>
#include <base/defines.h>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>


namespace ProfileEvents
{
    extern const Event JoinReorderMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXPERIMENTAL_FEATURE_ERROR;
}

/// Pack a `BitSet` (boost::dynamic_bitset) of relation ids into a native 32-bit mask.
/// Used by the DPsub hot path, where the relation count is guaranteed below 32 so every
/// set bit fits, allowing the per-CCP work to run on native integers instead of allocating
/// a `dynamic_bitset` for every plan it considers.
static UInt32 toMask(const BitSet & bits)
{
    UInt32 mask = 0;
    for (auto bit : bits)
    {
        chassert(bit < std::numeric_limits<UInt32>::digits);
        mask |= (static_cast<UInt32>(1) << bit);
    }
    return mask;
}

DPJoinEntry::DPJoinEntry(size_t id, std::optional<UInt64> rows, std::unordered_map<String, ColumnStats> column_stats_)
    : relations()
    , cost(0.0)
    , estimated_rows(rows)
    , column_stats(std::move(column_stats_))
    , relation_id(static_cast<int>(id))
{
    relations.set(id);
}

DPJoinEntry::DPJoinEntry(DPJoinEntryPtr lhs,
        DPJoinEntryPtr rhs,
        double cost_,
        std::optional<UInt64> cardinality_,
        JoinOperator join_operator_,
        JoinMethod join_method_)
    : relations(lhs->relations | rhs->relations)
    , left(std::move(lhs))
    , right(std::move(rhs))
    , cost(cost_)
    , estimated_rows(cardinality_)
    , join_operator(std::move(join_operator_))
    , join_method(join_method_)
{
    /// Merge column stats from both children, then update NDVs for equi-join key columns.
    column_stats = left->column_stats;
    column_stats.insert(right->column_stats.begin(), right->column_stats.end());

    for (const auto & predicate : join_operator.expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        if (!left_node.fromLeft() || !right_node.fromRight())
            continue;

        const auto & left_col = left_node.getColumnName();
        const auto & right_col = right_node.getColumnName();
        auto left_it = column_stats.find(left_col);
        auto right_it = column_stats.find(right_col);

        if (left_it != column_stats.end() && right_it != column_stats.end())
        {
            UInt64 min_ndv = std::min(left_it->second.num_distinct_values, right_it->second.num_distinct_values);
            left_it->second.num_distinct_values = min_ndv;
            right_it->second.num_distinct_values = min_ndv;
        }
    }

    /// Cap all NDVs at the estimated output rows.
    if (cardinality_)
    {
        for (auto & [_, stats] : column_stats)
            stats.num_distinct_values = std::min(stats.num_distinct_values, *cardinality_);
    }
}

bool DPJoinEntry::isLeaf() const { return !left && !right; }

/// Resolve a JoinActionRef to an INPUT node suitable for equivalence tracking.
/// Returns nullopt if the ref is not a simple single-relation INPUT column.
static std::optional<JoinActionRef> resolveInput(const JoinActionRef & ref)
{
    auto resolved = ref.resolveAliases();
    if (resolved.getNode()->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;
    if (!resolved.getSourceRelations().getSingleBit())
        return std::nullopt;
    return resolved;
}

void QueryGraph::buildColumnEquivalences()
{
    for (const auto & edge : edges)
    {
        if (!edge)
            continue;

        auto [op, lhs, rhs] = edge.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        auto lhs_resolved = resolveInput(lhs);
        auto rhs_resolved = resolveInput(rhs);
        if (!lhs_resolved || !rhs_resolved)
            continue;

        auto lhs_rel = lhs_resolved->getSourceRelations().getSingleBit();
        auto rhs_rel = rhs_resolved->getSourceRelations().getSingleBit();

        /// Skip predicates involving outer-joined relations: when a LEFT/RIGHT/FULL JOIN
        /// doesn't match, the outer side produces NULLs, so the equality doesn't hold
        /// for all rows and the transitive equivalence would be invalid.
        auto lhs_it = join_kinds.find(*lhs_rel);
        auto rhs_it = join_kinds.find(*rhs_rel);
        if ((lhs_it != join_kinds.end() && !isInner(lhs_it->second.second))
            || (rhs_it != join_kinds.end() && !isInner(rhs_it->second.second)))
            continue;

        if (outer_join_conditions.contains(edge))
            continue;

        column_equivalences.add(*lhs_resolved, *rhs_resolved);

        LOG_TRACE(&Poco::Logger::get("JoinOrderOptimizer"),
            "Column equivalence: relation {} `{}` = relation {} `{}`",
            *lhs_rel, lhs_resolved->getColumnName(), *rhs_rel, rhs_resolved->getColumnName());
    }
}

bool QueryGraph::areTransitivelyConnected(const BitSet & left, const BitSet & right) const
{
    for (const auto & [member, _] : column_equivalences.getMemberToClassMap())
    {
        auto member_rel = member.getSourceRelations().getSingleBit();

        if (!member_rel || !left.test(*member_rel))
            continue;

        auto equiv_class = column_equivalences.getClass(member);
        if (!equiv_class)
            continue;

        for (const auto & other : *equiv_class)
        {
            auto other_rel = other.getSourceRelations().getSingleBit();
            if (other_rel && right.test(*other_rel))
                return true;
        }
    }
    return false;
}

/// Post-process the join tree to remove redundant predicates and synthesize missing ones.
///
/// Walks bottom-up building equivalence classes from each join step's predicates.
/// At each step:
///   1. Remove predicates whose endpoints are already equivalent from child joins.
///      Non-redundant predicates are added to the equivalence classes immediately,
///      so later predicates at the same step can also be detected as redundant.
///   2. If no predicates remain (transitive-only join), synthesize one per equivalence
///      class spanning the left and right subtrees.
static void cleanupJoinPredicates(
    const DPJoinEntryPtr & root,
    const EquivalenceClasses<JoinActionRef> & column_equivalences)
{
    using EquivClasses = EquivalenceClasses<JoinActionRef>;

    std::function<EquivClasses(const DPJoinEntryPtr &)> process =
        [&](const DPJoinEntryPtr & entry) -> EquivClasses
    {
        if (entry->isLeaf())
            return {};

        /// Merge equivalence classes from both children.
        auto equiv = process(entry->left);
        equiv.merge(process(entry->right));

        /// Phase 1: Remove redundant predicates.
        auto & expressions = entry->join_operator.expression;
        bool is_inner = isInner(entry->join_operator.kind);

        std::erase_if(expressions, [&](const JoinActionRef & predicate)
        {
            auto [op, lhs, rhs] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                return false;

            auto lhs_resolved = resolveInput(lhs);
            auto rhs_resolved = resolveInput(rhs);
            if (!lhs_resolved || !rhs_resolved)
                return false;

            auto lhs_class = equiv.getClass(*lhs_resolved);
            auto rhs_class = equiv.getClass(*rhs_resolved);
            if (lhs_class && rhs_class && lhs_class == rhs_class)
            {
                auto lhs_rel = lhs_resolved->getSourceRelations().getSingleBit();
                auto rhs_rel = rhs_resolved->getSourceRelations().getSingleBit();
                LOG_TRACE(&Poco::Logger::get("JoinOrderOptimizer"),
                    "Removed redundant join predicate: relation {} `{}` = relation {} `{}`",
                    lhs_rel ? *lhs_rel : 0, lhs_resolved->getColumnName(),
                    rhs_rel ? *rhs_rel : 0, rhs_resolved->getColumnName());
                return true;
            }

            /// Only propagate equivalences from inner joins to the parent;
            /// outer join equality holds only for matching rows
            /// and would be invalid for NULL-padded non-matching rows.
            if (is_inner)
                equiv.add(*lhs_resolved, *rhs_resolved);
            return false;
        });

        /// Phase 2: Synthesize predicates for transitive-only joins.
        if (expressions.empty() && isInner(entry->join_operator.kind))
        {
            const auto & left_rels = entry->left->relations;
            const auto & right_rels = entry->right->relations;

            using ConstClassPtr = EquivClasses::ConstClassPtr;
            std::unordered_set<ConstClassPtr> visited;

            for (const auto & [member, _] : column_equivalences.getMemberToClassMap())
            {
                auto member_rel = member.getSourceRelations().getSingleBit();
                if (!member_rel || !left_rels.test(*member_rel))
                    continue;

                auto equiv_class = column_equivalences.getClass(member);
                if (!equiv_class || !visited.insert(equiv_class).second)
                    continue;

                for (const auto & other : *equiv_class)
                {
                    auto other_rel = other.getSourceRelations().getSingleBit();
                    if (!other_rel || !right_rels.test(*other_rel))
                        continue;

                    expressions.push_back(JoinActionRef::transform(
                        {member, other},
                        JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
                    equiv.add(member, other);

                    LOG_TRACE(&Poco::Logger::get("JoinOrderOptimizer"),
                        "Synthesized transitive predicate: relation {} `{}` = relation {} `{}`",
                        *member_rel, member.getColumnName(), *other_rel, other.getColumnName());
                    /// One predicate per equivalence class is enough for connectivity.
                    break;
                }
            }
        }

        return equiv;
    };

    process(root);
}

String DPJoinEntry::dump() const
{
    if (isLeaf())
        return fmt::format("Leaf({})", relation_id);
    return fmt::format("Join({})", fmt::join(relations, ","));
}

class JoinOrderOptimizer
{
public:
    JoinOrderOptimizer(QueryGraph query_graph_, const std::vector<JoinOrderAlgorithm> & enabled_algorithms_, UInt64 max_searched_plans_)
        : query_graph(std::move(query_graph_))
        , max_searched_plans(max_searched_plans_)
        , enabled_algorithms(enabled_algorithms_)
    {
        auto context = CurrentThread::tryGetQueryContext();
        if (context)
        {
            query_status = context->getProcessListElementSafe();
            interactive_cancel_callback = context->getInteractiveCancelCallback();
        }
    }

    std::shared_ptr<DPJoinEntry> solve();
private:
    void buildQueryGraph();

    template <typename DPTable, std::unsigned_integral Tuint>
    std::shared_ptr<DPJoinEntry> buildPhysicalPlan(const DPTable & dptable, const Tuint & S) const;
    std::shared_ptr<DPJoinEntry> solveDPsub();

    template <class Tuint, class TDPTable>
    friend class EnumeratorCheckerWithCosts;

    std::optional<UInt64> estimateCardinality(
        std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, double selectivity, JoinKind join_kind) const;

    /// Native-mask counterparts of the helpers above, used exclusively by the DPsub acceptor.
    /// They operate on `UInt32` relation masks and the data precomputed by `initDPsubScratch`,
    /// so the hot enumeration loop never constructs a `BitSet` (which heap-allocates). They are
    /// equivalent to the `BitSet` overloads but bypass per-CCP allocations entirely.
    void initDPsubScratch();
    std::optional<JoinKind> isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const;
    /// Returns the connecting (and, for two-relation joins, the early non-connecting) predicates,
    /// reusing an internal scratch buffer that is overwritten on every call.
    const std::vector<JoinActionRef *> & collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask);
    double computeSelectivityMask(const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask);

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, double> expression_selectivity;
    std::unordered_map<BitSet, DPJoinEntryPtr> dp_table;

    /** Precomputed native-mask view of the query graph for the DPsub hot path. Populated once per
    * `solveDPsub` run by `initDPsubScratch`. Each `BitSet` is packed into a `UInt` type, the outer-join
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
    };
    DPsubMaskData<UInt32> dpsub_data;

    const UInt64 max_searched_plans;

    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    LoggerPtr log = getLogger("JoinOrderOptimizer");

    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

std::optional<UInt64> JoinOrderOptimizer::estimateCardinality(
    std::optional<UInt64> left_rows, std::optional<UInt64> right_rows, double selectivity, JoinKind join_kind) const
{
    return estimateJoinCardinality(left_rows, right_rows, selectivity, join_kind);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinReorderMicroseconds);

    std::shared_ptr<DPJoinEntry> best_plan;

    for (const auto & algorithm : enabled_algorithms)
    {
        LOG_TRACE(log, "Solving join order using {} algorithm", toString(algorithm));
        switch (algorithm)
        {
            case JoinOrderAlgorithm::DPSUB:
                best_plan = solveDPsub();
                break;
            case JoinOrderAlgorithm::DPSIZE:
                best_plan = solveDPSizeJoinOrder(query_graph, max_searched_plans, query_status, interactive_cancel_callback);
                break;
            case JoinOrderAlgorithm::DPHYP:
                best_plan = solveDPHypJoinOrder(query_graph, max_searched_plans, query_status, interactive_cancel_callback);
                break;
            case JoinOrderAlgorithm::GREEDY:
                best_plan = solveGreedyJoinOrder(query_graph);
                if (!best_plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order with greedy algorithm");
                break;
        }

        if (best_plan)
            break;
    }

    if (!best_plan)
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Failed to find a valid join order, try adding 'greedy' algorithm as fallback to query_plan_optimize_join_order_algorithm setting.");

    LOG_TRACE(log, "Optimized join order in {:.2f} ms, best plan cost: {}, estimated cardinality: {}",
        static_cast<double>(watch.elapsed()) / 1000.0, best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown");

    return best_plan;
}

void JoinOrderOptimizer::initDPsubScratch()
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

    /// Outer-join restrictions: index by the null-supplying relation
    dpsub_data.restriction_by_rel.assign(num_relations, {});
    for (const auto & [rel, restriction] : query_graph.join_kinds)
    {
        if (rel >= num_relations)
            continue;
        auto & native = dpsub_data.restriction_by_rel[rel];
        native.required = toMask(restriction.first);
        native.kind = restriction.second;
        native.present = true;
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

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const
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

const std::vector<JoinActionRef *> & JoinOrderOptimizer::collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask)
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
            /// needs. This join is the lowest one that makes it applicable, so attach it here: into the
            /// correct join's ON condition.
            out.push_back(&edge);
        }
    }
    return out;
}

double JoinOrderOptimizer::computeSelectivityMask(
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
std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::buildPhysicalPlan(const DPTable & dptable, const TUInt & S) const
{
    auto& entry = dptable[S];
    if (!entry.left && !entry.right)
        return std::make_shared<DPJoinEntry>(std::countr_zero(S), entry.estimated_rows, entry.column_stats);

    JoinOperator join_operator(entry.kind, JoinStrictness::All, JoinLocality::Unspecified);
    /// A filter predicate applied at an outer join step must not go to the ON clause, where it
    /// would affect matching instead of filtering and let non-matching rows of the preserved side
    /// survive NULL-extended. Apply it after the join instead (see `solveGreedy`).
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
std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPsub()
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
        std::vector<JoinActionRef*> edges; // needed for physical plan generation
    };
    using DPTable = DPTable<DPEntry, Bitvector>;
    using Checker = EnumeratorCheckerWithCosts<DPTable, JoinOrderOptimizer>;
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

DPJoinEntryPtr optimizeJoinOrder(QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (query_graph.relation_stats.size() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderOptimizer: number of relations must be greater than 1");

    EquivalenceClasses<JoinActionRef> column_equivalences;
    if (optimization_settings.enable_join_transitive_predicates)
    {
        query_graph.buildColumnEquivalences();
        column_equivalences = query_graph.column_equivalences;
    }

    JoinOrderOptimizer reorderer(
        std::move(query_graph),
        optimization_settings.query_plan_optimize_join_order_algorithm,
        optimization_settings.query_plan_optimize_join_order_max_searched_plans);
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    if (optimization_settings.enable_join_transitive_predicates)
        cleanupJoinPredicates(best_plan, column_equivalences);
    return best_plan;
}

}
