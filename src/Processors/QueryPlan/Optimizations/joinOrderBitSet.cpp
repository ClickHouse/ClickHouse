#include <Processors/QueryPlan/Optimizations/joinOrderBitSet.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{

std::optional<JoinKind> isValidJoinOrder(
    const QueryGraph & query_graph,
    const BitSet & left_mask,
    const BitSet & right_mask)
{
    auto check = [&](const auto & lhs, const auto & rhs) -> std::optional<JoinKind>
    {
        auto rel_id = lhs.getSingleBit();
        if (rel_id.has_value())
        {
            auto it = query_graph.join_kinds.find(rel_id.value());
            if (it != query_graph.join_kinds.end())
            {
                if (isSubsetOf(it->second.first, rhs))
                    return it->second.second;
                return {};
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

std::vector<JoinActionRef *> getApplicableExpressions(
    QueryGraph & query_graph,
    const BitSet & left,
    const BitSet & right)
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

        auto pin_it = query_graph.outer_join_conditions.find(edge);
        if (pin_it != query_graph.outer_join_conditions.end())
        {
            /// ON-clause predicates of an outer join can be applied only when the
            /// null-supplying relation is joined. That relation appears as a singleton
            /// on one side of the join step (enforced by isValidJoinOrder), so the
            /// predicate becomes applicable exactly at that step.
            if (!joined_rels.test(pin_it->second))
                continue;
        }

        applicable.push_back(&edge);
    }
    return applicable;
}

/// Compute selectivity combining direct edges and transitive equivalence classes.
/// Direct edges and transitive equivalences may cover different columns between
/// the two relation sets, so both contribute to the overall selectivity.
double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges,
    const BitSet & left,
    const BitSet & right)
{
    double selectivity = DB::computeSelectivity(query_graph, dp_table, expression_selectivity, edges);

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
                max_ndv = std::max(max_ndv, getColumnStats(query_graph, dp_table, equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
            else if (right.test(*relation))
            {
                has_right = true;
                max_ndv = std::max(max_ndv, getColumnStats(query_graph, dp_table, equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
        }
        if (has_left && has_right && max_ndv > 0)
            selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    }

    return selectivity;
}

}
