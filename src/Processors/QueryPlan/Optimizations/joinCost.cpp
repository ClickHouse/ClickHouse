#include <Processors/QueryPlan/Optimizations/joinCost.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Interpreters/JoinOperator.h>
#include <vector>
#include <Common/logger_useful.h>

namespace DB
{

size_t estimateJoinCardinality(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    JoinKind join_kind)
{
    double joined_rows = std::max(left->estimated_rows * right->estimated_rows * selectivity, 1.0);

    switch (join_kind)
    {
        case JoinKind::Inner:
            [[fallthrough]];
        case JoinKind::Comma:
            [[fallthrough]];
        case JoinKind::Cross:
            return static_cast<size_t>(joined_rows);
        case JoinKind::Left:
            return static_cast<size_t>(std::max<double>(joined_rows, left->estimated_rows));
        case JoinKind::Right:
            return static_cast<size_t>(std::max<double>(joined_rows, right->estimated_rows));
        case JoinKind::Full:
            return static_cast<size_t>(std::max<double>(joined_rows, left->estimated_rows + right->estimated_rows));
        default:
            return static_cast<size_t>(joined_rows);
    }
}

double computeJoinCost(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity)
{
    double cost = selectivity * left->estimated_rows * right->estimated_rows;
    return left->cost + right->cost + cost;
}


static size_t getSingleBit(BaseRelsSet set)
{
    chassert(set.count() == 1);
    return std::countr_zero(set.to_ullong());
}


const ColumnStats * tryGetColumnStats(BaseRelsSet rels, const String & column_name, const std::vector<RelationStats> & relation_stats)
{
    if (rels.count() != 1)
        return nullptr;

    auto rel_id = getSingleBit(rels);
    if (rel_id >= relation_stats.size())
        return nullptr;

    auto it = relation_stats[rel_id].column_stats.find(column_name);
    if (it == relation_stats[rel_id].column_stats.end())
        return nullptr;

    return &it->second;

}

String formatJoinCondition(const JoinCondition & join_condition);

double estimateJoinSelectivity(
    const JoinOperator & join_operator,
    const std::vector<RelationStats> & relation_stats)
{
    double selectivity = 1.0;
    for (const auto & pred : join_operator.expression.condition.predicates)
    {
        if (pred.op != PredicateOperator::Equals && pred.op != PredicateOperator::NullSafeEquals)
            continue;

        const auto * left_stats = tryGetColumnStats(pred.left_node.getSourceRels(), pred.left_node.getDisplayName(), relation_stats);
        const auto * right_stats = tryGetColumnStats(pred.right_node.getSourceRels(), pred.right_node.getDisplayName(), relation_stats);
        LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: {} {}", __FILE__, __LINE__, left_stats ? left_stats->num_distinct_values : 0, right_stats ? right_stats->num_distinct_values : 0);

        UInt64 max_ndv = std::max(
            left_stats ? left_stats->num_distinct_values : 0,
            right_stats ? right_stats->num_distinct_values : 0);
        if (max_ndv)
            selectivity = std::min(selectivity, 1.0 / max_ndv);
    }

    LOG_TRACE(getLogger("optimizeJoin"), "Estimated selectivity for join operator {}: {}", formatJoinCondition(join_operator.expression.condition), selectivity);
    return selectivity;
}

}
