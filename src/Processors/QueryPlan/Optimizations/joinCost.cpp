#include <Processors/QueryPlan/Optimizations/joinCost.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Interpreters/JoinOperator.h>
#include <vector>

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

double estimateJoinSelectivity(
    const JoinOperator & join_operator,
    const std::vector<RelationStats> & relation_stats)
{
    UNUSED(join_operator);
    UNUSED(relation_stats);
    /// TODO
    return 0.1;
}

}
