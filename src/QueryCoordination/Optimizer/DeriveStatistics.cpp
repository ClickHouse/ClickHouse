#include <QueryCoordination/Optimizer/DeriveStatistics.h>


namespace DB
{

Statistics DeriveStatistics::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

Statistics DeriveStatistics::visitDefault()
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(ReadFromMergeTree & step)
{
    Statistics statistics;
    statistics.setOutputRowSize(step.getAnalysisResult().selected_rows);
    return statistics;
}

Statistics DeriveStatistics::visit(AggregatingStep & step)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;

    if (step.isFinal())
    {
        statistics.setOutputRowSize(std::max(size_t(1), input_row_size / 4)); /// fake agg
    }
    else
    {
        statistics.setOutputRowSize(std::max(size_t(1), input_row_size / 2));
    }
    return statistics;
}

Statistics DeriveStatistics::visit(MergingAggregatedStep & /*step*/)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size / 2);
    return statistics;
}

Statistics DeriveStatistics::visit(ExpressionStep & /*step*/)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size); /// TODO filter ?
    return statistics;
}

Statistics DeriveStatistics::visit(SortingStep & step)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    if (step.getLimit())
    {
        statistics.setOutputRowSize(std::min(input_row_size, size_t(step.getLimit())));
    }
    else
    {
        statistics.setOutputRowSize(input_row_size);
    }
    return statistics;
}


Statistics DeriveStatistics::visit(LimitStep & step)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    if (step.getLimit())
    {
        statistics.setOutputRowSize(std::min(input_row_size, size_t(step.getLimit())));
    }
    else
    {
        statistics.setOutputRowSize(input_row_size);
    }
    return statistics;
}

Statistics DeriveStatistics::visit(JoinStep & /*step*/)
{
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    /// TODO inner join, cross join
    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(UnionStep & /*step*/)
{
    size_t input_row_size = 0;
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
    size_t input_row_size = input_statistics.front().getOutputRowSize();

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

}
