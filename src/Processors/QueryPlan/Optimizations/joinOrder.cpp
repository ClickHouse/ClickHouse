#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Common/CurrentThread.h>

#include <algorithm>
#include <deque>
#include <functional>
#include <limits>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
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
#include <ranges>
#include <stack>
#include <unordered_map>
#include <vector>


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
        if ((lhs_it != join_kinds.end() && !isInner(lhs_it->second.kind))
            || (rhs_it != join_kinds.end() && !isInner(rhs_it->second.kind)))
            continue;

        /// Skip pinned predicates: they're conditionally applicable (only when the
        /// pin relations are joined first), so the equality doesn't hold unconditionally
        /// and the transitive equivalence cannot bypass the pin's constraint. Without
        /// this skip, `areTransitivelyConnected` would consider `(A,C)` connected via
        /// `a.w=c.w` even when that edge is pinned to require B, letting greedy commit
        /// to a dead-end first pair.
        if (auto pin_it = pinned.find(edge); pin_it != pinned.end() && pin_it->second.any())
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
    JoinOrderOptimizer(QueryGraph query_graph_, const std::vector<JoinOrderAlgorithm> & enabled_algorithms_)
        : query_graph(std::move(query_graph_))
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

    std::shared_ptr<DPJoinEntry> solveDPsize();
    std::shared_ptr<DPJoinEntry> solveGreedy();
    std::shared_ptr<DPJoinEntry> solveDPhyp();

    std::optional<JoinKind> isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const;
    std::vector<JoinActionRef *> getApplicableExpressions(const BitSet & left, const BitSet & right);

    double computeSelectivity(const JoinActionRef & edge);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right);
    size_t getColumnStats(BitSet rels, const String & column_name);

    /// Periodically called from potentially long running optimization to check time limits and send progress
    void checkLimits();

    /// Try to build the best join plan between left_rels and right_rels.
    /// Updates dp_table if a better plan is found.
    void tryJoin(const BitSet & left_rels, const BitSet & right_rels);

    /// Core plan-building logic shared by DPsize and DPhyp.
    /// Computes selectivity and cost for the given predicates, and updates dp_table if this plan is better.
    /// Returns the new entry if dp_table was updated, nullptr otherwise.
    DPJoinEntryPtr evaluateJoin(
        const DPJoinEntryPtr & left,
        const DPJoinEntryPtr & right,
        JoinKind join_kind,
        std::vector<JoinActionRef *> & predicates);

    /// DPhyp helpers
    void buildHyperedges();
    BitSet getNeighborhood(const BitSet & node_set) const;

    /// DPhyp enumeration functions from "Dynamic Programming Strikes Back"
    /// (Moerkotte & Neumann, SIGMOD 2008), Section 3.
    void emitCsg(const BitSet & csg);                       /// Generate complement seeds for a connected subgraph
    void enumerateCsgRec(const BitSet & csg, const BitSet & exclusion); /// Grow the primary connected subgraph
    void emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg); /// Evaluate a csg-cmp pair
    void enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion); /// Grow the complement

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, bool> applied;
    std::unordered_map<JoinActionRef, double> expression_selectivity;
    std::unordered_map<BitSet, DPJoinEntryPtr> dp_table;

    /// A hyperedge in the join graph connecting a set of left relations to a set of right relations.
    /// For simple binary predicates (A.x = B.y), |left| = |right| = 1.
    /// For complex predicates (A.x = B.y + C.z), left and/or right may span multiple relations.
    struct Hyperedge
    {
        BitSet left;
        BitSet right;
    };

    /// DPhyp hyperedge representation (built lazily by buildHyperedges)
    std::vector<Hyperedge> hyperedges;
    std::vector<std::vector<size_t>> node_to_edge_ids; /// node index -> hyperedge indices

    /// Set by `tryJoin` when it encounters a single-table or constant predicate inside the join edges
    /// that `dphyp` does not yet know how to attach. `solveDPhyp` returns `nullptr` so the fallback
    /// algorithm chain (e.g. `dphyp,greedy`) can produce a valid plan.
    bool dphyp_unsupported_predicate = false;

    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    LoggerPtr log = getLogger("JoinOrderOptimizer");

    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

void JoinOrderOptimizer::checkLimits()
{
    if (query_status)
        query_status->checkTimeLimit();
    if (interactive_cancel_callback)
        interactive_cancel_callback();
}

size_t JoinOrderOptimizer::getColumnStats(BitSet rels, const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        /// Look up NDV from the dp_table entry's column_stats (propagated through joins).
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
            auto col_it = it->second->column_stats.find(column_name);
            if (col_it != it->second->column_stats.end())
                return col_it->second.num_distinct_values;
            return it->second->estimated_rows.value_or(0);
        }
        return 0;
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
    const auto & col_stats = relation_stat.column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end())
        return it->second.num_distinct_values;
    return relation_stat.estimated_rows.value_or(0);
}

double JoinOrderOptimizer::computeSelectivity(const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge, 1.0);
    auto & selectivity = it->second;
    if (!inserted)
        return selectivity;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return 1.0;

    UInt64 lhs_ndv = getColumnStats(lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    return selectivity;
}

double JoinOrderOptimizer::computeSelectivity(const std::vector<JoinActionRef *> & edges)
{
    double selectivity = 1.0;
    for (const auto & edge : edges)
        selectivity = std::min(selectivity, computeSelectivity(*edge));
    return selectivity;
}

/// Compute selectivity combining direct edges and transitive equivalence classes.
/// Direct edges and transitive equivalences may cover different columns between
/// the two relation sets, so both contribute to the overall selectivity.
double JoinOrderOptimizer::computeSelectivity(
    const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right)
{
    double selectivity = computeSelectivity(edges);

    /// Also account for transitively-equivalent columns spanning both sides.
    using ConstClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;
    std::unordered_set<ConstClassPtr> visited;

    for (const auto & [member, _] : query_graph.column_equivalences.getMemberToClassMap())
    {
        auto member_rel = member.getSourceRelations().getSingleBit();
        if (!member_rel || !left.test(*member_rel))
            continue;

        auto equiv_class = query_graph.column_equivalences.getClass(member);
        if (!equiv_class || !visited.insert(equiv_class).second)
            continue;

        /// Find the maximum NDV across all members of this class that belong
        /// to either side of the join. This is equivalent to evaluating all
        /// (left_member, right_member) pairs and taking the minimum selectivity,
        /// since min(1/max(l,r)) = 1/max(all l's and r's).
        size_t max_ndv = 0;
        bool has_left = false;
        bool has_right = false;
        for (const auto & equiv_member : *equiv_class)
        {
            auto relation = equiv_member.getSourceRelations().getSingleBit();
            if (!relation)
                continue;
            if (left.test(*relation))
            {
                has_left = true;
                max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
            else if (right.test(*relation))
            {
                has_right = true;
                max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
        }
        if (has_left && has_right && max_ndv > 0)
            selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    }

    return selectivity;
}


static std::optional<UInt64> estimateJoinCardinality(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    if (!left->estimated_rows || !right->estimated_rows)
        return {};

    double lhs = static_cast<double>(left->estimated_rows.value());
    double rhs = static_cast<double>(right->estimated_rows.value());

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, lhs + rhs);

    /// Use >= to avoid undefined behavior when joined_rows is very close to max UInt64
    /// Due to floating point precision, a value slightly less than max when compared
    /// as double could still overflow when cast to UInt64
    if (joined_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

static double computeJoinCost(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity)
{
    return left->cost + right->cost + selectivity * static_cast<double>(left->estimated_rows.value_or(1)) * static_cast<double>(right->estimated_rows.value_or(1));
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
            case JoinOrderAlgorithm::DPSIZE:
                best_plan = solveDPsize();
                break;
            case JoinOrderAlgorithm::DPHYP:
                best_plan = solveDPhyp();
                break;
            case JoinOrderAlgorithm::GREEDY:
                best_plan = solveGreedy();
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


std::vector<JoinActionRef *> JoinOrderOptimizer::getApplicableExpressions(const BitSet & left, const BitSet & right)
{
    std::vector<JoinActionRef *> applicable;

    BitSet joined_rels = left | right;
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;
        const auto & edge_sources = edge.getSourceRelations();
        if (!isSubsetOf(edge_sources, joined_rels))
            continue;

        auto pin_it = query_graph.pinned.find(edge);
        if (pin_it != query_graph.pinned.end())
        {
            /** We pin the expression in two cases:
              * 1. The expression is part of an OUTER JOIN ON clause — pinned to the
              *    NULL-supplying side.
              * 2. The expression of an INNER join sits above a child outer-join
              *    boundary it cannot legally be pushed past — pinned to the
              *    null-supplying relations of every such boundary it crosses.
              * The pin set is the relations that MUST all be in `joined_rels`
              * before the edge is applicable, so check subset.
              */
            if (!isSubsetOf(pin_it->second, joined_rels))
                continue;
        }

        applicable.push_back(&edge);
    }
    return applicable;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
    /// Discard any partial state left by an earlier algorithm in the fallback chain
    /// (e.g. `dphyp,greedy`) so cost-model lookups via `getColumnStats` only see
    /// entries built by this run. `expression_selectivity` is cleared along with
    /// `dp_table` because multi-relation predicates resolve NDV through it.
    dp_table.clear();
    expression_selectivity.clear();

    std::deque<std::shared_ptr<DPJoinEntry>> components;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        components.push_back(std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats));
    }

    std::vector<JoinActionRef *> applied_edge;
    /// Iteratively join components until we have a single plan
    while (components.size() > 1)
    {
        std::shared_ptr<DPJoinEntry> best_plan = nullptr;
        size_t best_i = 0;
        size_t best_j = 0;

        /// Try all pairs of components
        for (size_t i = 0; i < components.size(); i++)
        {
            for (size_t j = i + 1; j < components.size(); j++)
            {
                auto left = components[i];
                auto right = components[j];

                auto join_kind = isValidJoinOrder(left->relations, right->relations);
                if (!join_kind)
                    continue;

                auto edge = getApplicableExpressions(left->relations, right->relations);
                bool connected = !edge.empty()
                    || query_graph.areTransitivelyConnected(left->relations, right->relations);
                if (!connected && best_plan)
                    continue;

                auto selectivity = computeSelectivity(edge, left->relations, right->relations);
                auto current_cost = computeJoinCost(left, right, selectivity);
                if (!best_plan || current_cost < best_plan->cost)
                {
                    if (connected && join_kind == JoinKind::Cross)
                        join_kind = JoinKind::Inner;
                    auto cardinality = estimateJoinCardinality(left, right, selectivity, join_kind.value());
                    JoinOperator join_operator(
                        join_kind.value(), JoinStrictness::All, JoinLocality::Unspecified,
                        std::ranges::to<std::vector>(edge | std::views::transform([](const auto * e) { return *e; })));
                    applied_edge = std::move(edge);
                    best_plan = std::make_shared<DPJoinEntry>(left, right, current_cost, cardinality, std::move(join_operator));
                    best_i = i;
                    best_j = j;
                }
            }
        }

        /// If no valid join was found, add a cross product between smallest relations
        if (!best_plan)
        {
            /// Find two smallest components
            UInt64 first_best = std::numeric_limits<UInt64>::max();
            best_i = 0;
            UInt64 second_best = std::numeric_limits<UInt64>::max();
            best_j = 0;

            for (size_t idx = 0; idx < components.size(); idx++)
            {
                auto & component = components[idx];
                UInt64 estimated_rows = component->estimated_rows.value_or(std::numeric_limits<UInt64>::max() - 1);
                if (estimated_rows < first_best)
                {
                    std::tie(second_best, best_j) = std::tie(first_best, best_i);
                    std::tie(first_best, best_i) = std::tie(estimated_rows, idx);
                }
                else if (estimated_rows < second_best)
                {
                    std::tie(second_best, best_j) = std::tie(estimated_rows, idx);
                }
            }

            if (best_i == best_j)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find two smallest components");
            if (!isValidJoinOrder(components[best_i]->relations, components[best_j]->relations))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join restriction violated");

            auto cost = computeJoinCost(components[best_i], components[best_j], 1.0);
            auto cardinality = estimateJoinCardinality(components[best_i], components[best_j], 1.0);
            JoinOperator join_operator(JoinKind::Cross, JoinStrictness::All, JoinLocality::Unspecified);
            /// Use left: min idx, right: max idx to keep original order order of joins
            /// We will swap tables later if needed
            best_plan = std::make_shared<DPJoinEntry>(components[std::min(best_i, best_j)], components[std::max(best_i, best_j)], cost, cardinality, join_operator);
            applied_edge.clear();
        }

        if (best_i == best_j)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find components to join");

        LOG_TEST(log, "Best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, join operator: {}",
            best_plan->dump(), best_plan->left->dump(), best_plan->right->dump(),
            best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown",
            best_plan->join_operator.dump());

        /// replace the two components with the best plan
        components.erase(components.begin() + std::max(best_i, best_j));
        components.erase(components.begin() + std::min(best_i, best_j));
        components.push_front(best_plan);
        dp_table[best_plan->relations] = best_plan;

        for (auto * edge : applied_edge)
            *edge = nullptr;
    }

    for (auto * edge : applied_edge)
        *edge = nullptr;

    auto non_applied_edges = std::views::filter(query_graph.edges, [](auto & edge) { return bool(edge); });
    if (!non_applied_edges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some expressions was not applied: [{}]",
            fmt::join(non_applied_edges | std::views::take(5) | std::views::transform(&JoinActionRef::dump), ", "));

    return components.at(0);
}


/// Checks if predicate has sources from both left and right sets
static bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right)
{
    const auto & participating = predicate->getSourceRelations();
    return areIntersecting(participating, left) && areIntersecting(participating, right);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPsize()
{
    const size_t total_relations_count = query_graph.relation_stats.size();

    /// Components by size (index 0 is not used that why the size is N+1)
    std::vector<std::unordered_map<BitSet, DPJoinEntryPtr>> components(total_relations_count + 1);

    /// Populate DP table for components of size=1.
    /// Also reset the per-edge selectivity cache so an earlier algorithm in the
    /// fallback chain cannot leak cached `1.0` defaults from a partial `dp_table`.
    dp_table.clear();
    expression_selectivity.clear();
    for (size_t i = 0; i < total_relations_count; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats);
        components[1][entry->relations] = entry;
        dp_table[entry->relations] = entry;
    }

    /// Iteratively build components of size from 2 to N
    for (size_t component_size = 2; component_size <= total_relations_count; ++component_size)
    {
        for (size_t smaller_component_size = 1; smaller_component_size <= component_size / 2; ++smaller_component_size)
        {
            const size_t bigger_component_size = component_size - smaller_component_size;

            for (const auto & [_, right] : components[smaller_component_size])
            {
                checkLimits();

                for (const auto & [_, left] : components[bigger_component_size])
                {
                    /// Do components overlap?
                    if (left->relations & right->relations)
                        continue;

                    /// If both components are of the same size then check each pair just once, not twice
                    if (smaller_component_size == bigger_component_size && *left->relations.begin() > *right->relations.begin())
                        continue;

                    auto join_kind = isValidJoinOrder(left->relations, right->relations);
                    if (!join_kind)
                        continue;

                    /// FIXME: Restrict to Inner joins for now because isValidJoinOrder seems to not handle non-Inner joins with swapped inputs correctly
                    if (*join_kind != JoinKind::Inner)
                        continue;

                    auto applicable_edge = getApplicableExpressions(left->relations, right->relations);
                    /// Only leave the edges that connect left and right.
                    /// DPsize also includes non-connecting predicates (single-table filters) at the earliest
                    /// stage (component_size == 2), unlike DPhyp which handles them separately via the hyperedge graph.
                    std::vector<JoinActionRef *> edge;
                    for (auto & edge_it : applicable_edge)
                    {
                        if (connects(edge_it, left->relations, right->relations))
                        {
                            LOG_TEST(log, "Adding predicate connecting {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else if ((edge_it->fromLeft() || edge_it->fromRight() || edge_it->fromNone()) && component_size == 2)
                        {
                            LOG_TEST(log, "Adding early non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else
                        {
                            LOG_TEST(log, "Skipping non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                        }
                    }

                    bool connected = !edge.empty()
                        || query_graph.areTransitivelyConnected(left->relations, right->relations);

                    LOG_TEST(log, "Considering join between {} and {}, predicates count: {}, connected: {}",
                        left->dump(), right->dump(), edge.size(), connected);

                    if (!connected)
                        continue;

                    auto new_entry = evaluateJoin(left, right, *join_kind, edge);
                    if (new_entry)
                        components[component_size][new_entry->relations] = new_entry;
                }
            }
        }
    }

    auto best_full_plan = dp_table.find(BitSet::allSet(total_relations_count));
    if (best_full_plan != dp_table.end())
        return best_full_plan->second;

    LOG_TRACE(log, "Failed to find best plan using DPsize algorithm");
    return nullptr;
}

void JoinOrderOptimizer::tryJoin(const BitSet & left_rels, const BitSet & right_rels)
{
    auto left_entry = dp_table.find(left_rels);
    if (left_entry == dp_table.end())
        return;

    auto right_entry = dp_table.find(right_rels);
    if (right_entry == dp_table.end())
        return;

    auto join_kind = isValidJoinOrder(left_rels, right_rels);
    if (!join_kind)
        return;

    /// Restrict to inner joins for now (same as DPsize FIXME)
    if (*join_kind != JoinKind::Inner)
        return;

    auto applicable_predicates = getApplicableExpressions(left_rels, right_rels);
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
        /// unsupported and let `solveDPhyp` return `nullptr` so the fallback chain runs.
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

    evaluateJoin(left_entry->second, right_entry->second, *join_kind, connecting_predicates);
}

DPJoinEntryPtr JoinOrderOptimizer::evaluateJoin(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    std::vector<JoinActionRef *> & predicates)
{
    auto selectivity = computeSelectivity(predicates, left->relations, right->relations);
    auto new_cost = computeJoinCost(left, right, selectivity);

    const BitSet combined_rels = left->relations | right->relations;
    auto current_best = dp_table.find(combined_rels);
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
        return nullptr;

    /// Transitively connected pairs are inner joins; their predicate is synthesized later.
    bool connected = !predicates.empty()
        || query_graph.areTransitivelyConnected(left->relations, right->relations);
    auto effective_kind = (connected && join_kind == JoinKind::Cross) ? JoinKind::Inner : join_kind;
    auto cardinality = estimateJoinCardinality(left, right, selectivity, effective_kind);
    JoinOperator join_operator(
        effective_kind, JoinStrictness::All, JoinLocality::Unspecified,
        std::ranges::to<std::vector>(predicates | std::views::transform([](const auto * p) { return *p; })));
    auto new_entry = std::make_shared<DPJoinEntry>(left, right, new_cost, cardinality, std::move(join_operator));

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

/// Build the hyperedge representation of the join graph used by DPhyp.
/// Each join predicate becomes a hyperedge (left_rels, right_rels).
/// Column equivalence classes add synthetic edges for transitively-connected pairs.
/// The adjacency index `node_to_edge_ids` maps each relation to the hyperedges that touch it.
void JoinOrderOptimizer::buildHyperedges()
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
BitSet JoinOrderOptimizer::getNeighborhood(const BitSet & node_set) const
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
static void forEachNonEmptySubset(const BitSet & mask, F && func)
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
        func(subset);
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
void JoinOrderOptimizer::emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg)
{
    if (dphyp_unsupported_predicate)
        return;
    LOG_TEST(log, "DPhyp: emitCsgCmp({{ {} }}, {{ {} }})",
        fmt::join(left_csg, ","), fmt::join(right_csg, ","));
    tryJoin(left_csg, right_csg);
}

/// Recursively extend complement S2 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
void JoinOrderOptimizer::enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;
    checkLimits();

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
        BitSet extended_complement = complement | extension;
        if (dp_table.contains(extended_complement))
            emitCsgCmp(csg, extended_complement);
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S2, X)).
    BitSet incremental_exclusion = exclusion | complement_neighborhood;
    forEachNonEmptySubset(complement_neighborhood, [&](const BitSet & extension)
    {
        enumerateCmpRec(csg, complement | extension, incremental_exclusion);
    });
}

/// Generate all complement seeds for a given connected subgraph S1.
/// Seeds are single neighbor nodes, processed in descending index order so that
/// `enumerateCmpRec` only extends complements toward higher-indexed nodes,
/// ensuring each (S1, S2) pair is enumerated exactly once.
///
/// `exclusion` (paper: X) = S1 | B_min(S1), where B_min(S1) = {v : v < min(S1)}.
/// B_min excludes all relations ordered before the smallest relation in S1.
/// This is the key mechanism that prevents generating symmetric pairs:
/// the complement can only contain relations ordered after the CSG's minimum.
void JoinOrderOptimizer::emitCsg(const BitSet & csg)
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

    for (auto it = neighbor_nodes.rbegin(); it != neighbor_nodes.rend(); ++it)
    {
        BitSet single_node;
        single_node.set(*it);
        emitCsgCmp(csg, single_node);
        enumerateCmpRec(csg, single_node, exclusion);
    }
}

/// Recursively extend connected subgraph S1 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
/// For each connected extension found in dp_table, calls `emitCsg` to generate complements.
void JoinOrderOptimizer::enumerateCsgRec(const BitSet & csg, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;
    checkLimits();

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
        BitSet extended_csg = csg | extension;
        if (dp_table.contains(extended_csg))
            emitCsg(extended_csg);
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S1, X)).
    BitSet extended_exclusion = exclusion | neighborhood;
    forEachNonEmptySubset(neighborhood, [&](const BitSet & extension)
    {
        enumerateCsgRec(csg | extension, extended_exclusion);
    });
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPhyp()
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
        BitSet seed;
        seed.set(static_cast<size_t>(i));

        LOG_TEST(log, "DPhyp: === seed {} ===", i);
        emitCsg(seed);
        exclusion.set(i, false);
        enumerateCsgRec(seed, exclusion);
    }

    if (dphyp_unsupported_predicate)
    {
        LOG_TRACE(log, "DPhyp encountered an unsupported predicate, falling back");
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

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const
{
    auto check = [&](const auto & lhs, const auto & rhs) -> std::optional<JoinKind>
    {
        auto rel_id = lhs.getSingleBit();
        if (rel_id.has_value())
        {
            auto it = query_graph.join_kinds.find(rel_id.value());
            if (it != query_graph.join_kinds.end())
            {
                const auto & restriction = it->second;
                if (!isSubsetOf(restriction.required_partners, rhs))
                    return {};
                if ((rhs & restriction.forbidden_partners).any())
                    return {};
                return restriction.kind;
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
    {
        /// When original join stored a Left/Full kind for the left relation,
        /// and it now appears on the left side of reordered join, reverse the kind
        left_join_type = isLeftOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    }
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
    /// Allow FULL join as it's restricted to table swapping and no reordering
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflict, join is not possible:
    /// FROM t1 LEFT JOIN t2 LEFT JOIN t3
    /// t1 -> Inner, t2 -> Left, t3 -> Left
    /// Cannot do (t2 x t3)
    return {};
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

    JoinOrderOptimizer reorderer(std::move(query_graph), optimization_settings.query_plan_optimize_join_order_algorithm);
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    if (optimization_settings.enable_join_transitive_predicates)
        cleanupJoinPredicates(best_plan, column_equivalences);
    return best_plan;
}

}
