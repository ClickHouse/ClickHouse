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
    // Basic cardinality estimation for different join types
    double joined_rows = left->estimated_rows * right->estimated_rows * selectivity;

    switch (join_kind)
    {
        case JoinKind::Inner:
        case JoinKind::Cross:
            return static_cast<size_t>(joined_rows);

        case JoinKind::Left:
            // For LEFT JOIN, we have at least as many rows as the left side
            return static_cast<size_t>(std::max(joined_rows, static_cast<double>(left->estimated_rows)));

        case JoinKind::Right:
            // For RIGHT JOIN, we have at least as many rows as the right side
            return static_cast<size_t>(std::max(joined_rows, static_cast<double>(right->estimated_rows)));

        case JoinKind::Full:
            // For FULL JOIN, we have at least as many rows as both sides combined
            return static_cast<size_t>(std::max(joined_rows,
                    static_cast<double>(left->estimated_rows + right->estimated_rows)));

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
    return 0.8;
}

}
