#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Common/CurrentThread.h>

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <base/defines.h>


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

LoggerPtr getJoinOrderOptimizerLogger()
{
    static LoggerPtr log = getLogger("JoinOrderOptimizer");
    return log;
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
    QueryGraph query_graph;
    const UInt64 max_searched_plans;
    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

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
                best_plan = solveDPSubJoinOrder(query_graph);
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
