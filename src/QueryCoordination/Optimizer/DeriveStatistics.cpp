#include <DataTypes/DataTypeDateTime.h>
#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>


namespace DB
{

Statistics DeriveStatistics::visit(QueryPlanStepPtr step)
{
    LOG_TRACE(log, "collect statistics for step {}", step->getName());
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
    Statistics scan_stats;
    scan_stats.setOutputRowSize(step.getAnalysisResult().selected_rows);
    /// TODO add column statistics
    /// step.getRealColumnNames()
    if (step.getStorageID().table_name == "student")
    {
        scan_stats.addColumnStatistics("id", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));
        scan_stats.addColumnStatistics("name", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 5.0, std::make_shared<DataTypeString>()));
        scan_stats.addColumnStatistics("event_time", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 8.0, std::make_shared<DataTypeDateTime>()));
        scan_stats.addColumnStatistics("city", std::make_shared<ColumnStatistics>(0.0, 0.0, 3.0, 14.0, std::make_shared<DataTypeString>()));
        scan_stats.addColumnStatistics("city_code", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));

    }
    else
    {
        scan_stats.addColumnStatistics("id", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));
        scan_stats.addColumnStatistics("event_time", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 8.0, std::make_shared<DataTypeDateTime>()));
        scan_stats.addColumnStatistics("score", std::make_shared<ColumnStatistics>(50.0, 96.0, 8.0, 8.0, std::make_shared<DataTypeInt32>()));
    }


    Statistics statistics;
    if (!step.getFilters().empty())
    {
        for (auto & filter : step.getFilters()) /// TODO and
            statistics = PredicateStatsCalculator::calculateStatistics(filter, scan_stats);
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
    chassert(input_statistics.size() == 1);
    Statistics statistics = PredicateStatsCalculator::calculateStatistics(step.getExpression(), input_statistics.front());
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
