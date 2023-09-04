#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>

namespace DB
{

Statistics PredicateStatsCalculator::calculateStatistics(const ActionsDAGPtr & predicates, const StatisticsList & inputs)
{
    if (!predicates) {
        return statistics;
    }

    // The time-complexity of PredicateStatisticsCalculatingVisitor OR row-count is O(2^n), n is OR number
    if (countDisConsecutiveOr(predicate, 0, false) > StatisticsEstimateCoefficient.DEFAULT_OR_OPERATOR_LIMIT) {
        return predicate.accept(new LargeOrCalculatingVisitor(statistics), null);
    } else {
        return predicate.accept(new BaseCalculatingVisitor(statistics), null);
    }
}

}
