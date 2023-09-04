#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>


namespace DB
{

Statistics DeriveStatistics::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

Statistics DeriveStatistics::visitDefault()
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize(); /// TODO source step

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(ReadFromMergeTree & step)
{
    Statistics statistics;
    statistics.setOutputRowSize(step.getAnalysisResult().selected_rows);
    /// TODO add column statistics
    /// step.getRealColumnNames()
    /// statistics.addColumnStatistics();
    if (!step.getFilters().empty())
    {
        for (auto & filter : step.getFilters()) /// TODO and
            PredicateStatsCalculator::calculateStatistics(filter, input_statistics);
    }
    return statistics;
}

Statistics DeriveStatistics::visit(ExpressionStep & /*step*/)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(FilterStep & step)
{
    Statistics statistics = PredicateStatsCalculator::calculateStatistics(step.getExpression(), input_statistics);
    return statistics;
}

Statistics DeriveStatistics::visit(AggregatingStep & step)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;

    if (step.isFinal())
    {
        statistics.setOutputRowSize(std::max(1.0, input_row_size / 4)); /// fake agg
    }
    else
    {
        statistics.setOutputRowSize(std::max(1.0, input_row_size / 2));
    }
    return statistics;
}

Statistics DeriveStatistics::visit(MergingAggregatedStep & /*step*/)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size / 2);
    return statistics;
}

Statistics DeriveStatistics::visit(SortingStep & step)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    if (step.getLimit())
    {
        statistics.setOutputRowSize(std::min(input_row_size, static_cast<Float64>(step.getLimit())));
    }
    else
    {
        statistics.setOutputRowSize(input_row_size);
    }
    return statistics;
}


Statistics DeriveStatistics::visit(LimitStep & step)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    if (step.getLimit())
    {
        statistics.setOutputRowSize(std::min(input_row_size, static_cast<Float64>(step.getLimit())));
    }
    else
    {
        statistics.setOutputRowSize(input_row_size);
    }
    return statistics;
}

Statistics DeriveStatistics::visit(JoinStep & /*step*/)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    /// TODO inner join, cross join
    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(UnionStep & /*step*/)
{
    Float64 input_row_size = 0;
    for (const auto & input_statistic : input_statistics)
    {
        input_row_size += input_statistic.getOutputRowSize();
    }

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(ExchangeDataStep & /*step*/)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

}
