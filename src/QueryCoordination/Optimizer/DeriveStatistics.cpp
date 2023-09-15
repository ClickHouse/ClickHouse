#include <DataTypes/DataTypeDateTime.h>
#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>


namespace DB
{

Statistics DeriveStatistics::visit(QueryPlanStepPtr step)
{
    LOG_TRACE(log, "collect statistics for step {}", step->getName());
    return Base::visit(step);
}

Statistics DeriveStatistics::visitDefault(QueryPlanStepPtr step)
{
    Float64 input_row_size = input_statistics.front().getOutputRowSize(); /// TODO source step

    if (auto * source_step = typeid_cast<ISourceStep *>(step.get()))
    {
        source_step->getOutputStream().header
    }
    else
    {

    }

    Statistics statistics;
    statistics.setOutputRowSize(input_row_size);
    return statistics;
}

Statistics DeriveStatistics::visit(ReadFromMergeTree & step)
{
    Statistics statistics;
    statistics.setOutputRowSize(step.getAnalysisResult().selected_rows);

//    if (step.getStorageID().table_name == "student")
//    {
//        statistics.addColumnStatistics("id", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));
//        statistics.addColumnStatistics("name", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 5.0, std::make_shared<DataTypeString>()));
//        statistics.addColumnStatistics("event_time", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 8.0, std::make_shared<DataTypeDateTime>()));
//        statistics.addColumnStatistics("city", std::make_shared<ColumnStatistics>(0.0, 0.0, 3.0, 14.0, std::make_shared<DataTypeString>()));
//        statistics.addColumnStatistics("city_code", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));
//
//    }
//    else
//    {
//        statistics.addColumnStatistics("id", std::make_shared<ColumnStatistics>(1.0, 5.0, 5.0, 4.0, std::make_shared<DataTypeInt32>()));
//        statistics.addColumnStatistics("event_time", std::make_shared<ColumnStatistics>(0.0, 0.0, 5.0, 8.0, std::make_shared<DataTypeDateTime>()));
//        statistics.addColumnStatistics("score", std::make_shared<ColumnStatistics>(50.0, 96.0, 8.0, 8.0, std::make_shared<DataTypeInt32>()));
//    }

    /// add column statistics
    for (auto & column : step.getRealColumnNames())
    {
        statistics.addColumnStatistics(column, ColumnStatistics::unknown());
    }

    /// For action_dags in prewhere do not contains all output nodes,
    /// we should append other nodes to statistics
    auto append_column_stats = [&step](Statistics & statistics_)
    {
        for (auto & column : step.getOutputStream().header.getNames())
        {
            if (!statistics_.getColumnStatistics(column))
                statistics_.addColumnStatistics(column, ColumnStatistics::unknown());
        }
    };

    /// 1. calculate for prewhere filters
    if (step.getPrewhereInfo())
    {
        auto prewhere_info = step.getPrewhereInfo();
        if (prewhere_info->row_level_filter)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->row_level_filter, prewhere_info->row_level_column_name, statistics);

            statistics.removeColumnStatistics(prewhere_info->row_level_column_name);
            append_column_stats(statistics);
        }

        if (prewhere_info->prewhere_actions)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name, statistics);

            if (prewhere_info->remove_prewhere_column)
                statistics.removeColumnStatistics(prewhere_info->prewhere_column_name);
            append_column_stats(statistics);
        }
    }

    /// 2. calculate for pushed down filters
    for (size_t i=0; i< step.getFilters().size(); i++)
    {
        auto & predicate = step.getFilters()[i];
        auto & predicate_node_name = step.getFilterNodes().nodes[i]->result_name;
        statistics = PredicateStatsCalculator::calculateStatistics(predicate, predicate_node_name, statistics);
    }

    return statistics;
}

Statistics DeriveStatistics::visit(ExpressionStep & step)
{
    chassert(input_statistics.size() == 1);
    const auto & action_dag = step.getExpression();
    Statistics statistics = ExpressionStatsCalculator::calculateStatistics(action_dag, input_statistics.front());
    return statistics;
}

Statistics DeriveStatistics::visit(FilterStep & step)
{
    chassert(input_statistics.size() == 1);
    Statistics statistics
        = PredicateStatsCalculator::calculateStatistics(step.getExpression(), step.getFilterColumnName(), input_statistics.front());
    return statistics;
}

Statistics DeriveStatistics::visit(AggregatingStep & step)
{
    Statistics statistics;

    Names input_names = step.getInputStreams().front().header.getNames();
    Names output_names = step.getOutputStream().header.getNames();

    const auto & input = input_statistics.front();
    for (const auto & name : input_names)
        chassert(input.getColumnStatistics(name));

    /// 1. initialize statistics
    chassert(input_names.size() == output_names.size());
    for (size_t i = 0; i < input_names.size(); i++)
    {
        /// The input name and output name are one-to-one correspondences.
        chassert(input.getColumnStatistics(input_names[i]));

        auto column_stats = input.getColumnStatistics(input_names[i]);
        statistics.addColumnStatistics(output_names[i], column_stats->clone());
    }

    /// 2. calculate selectivity
    const auto & aggregate_keys = step.getParams().keys;
    if (statistics.hasUnknownColumn())
    {
        /// Estimate by multiplying some coefficient
        Float64 selectivity = 0.2; /// TODO add to settings
        for (size_t i = 1; i < aggregate_keys.size(); i++)
        {
            if (selectivity * 1.1 > 1.0)
                break;
            selectivity *= 1.1; /// TODO add to settings
        }
        statistics.setOutputRowSize(selectivity * input.getOutputRowSize());
    }
    else
    {
        /// Estimate by ndv
        Float64 selectivity = 1.0;
        for (size_t i = 0; i < aggregate_keys.size(); i++)
        {
            auto aggregate_key_stats = statistics.getColumnStatistics(aggregate_keys[i]);
            chassert(aggregate_key_stats);

            Float64 ndv = aggregate_key_stats->getNdv();
            auto aggregate_key_selectivity = ndv / input.getOutputRowSize();

            if (i == 0)
                selectivity = aggregate_key_selectivity;
            else
            {
                if (selectivity + aggregate_key_selectivity * selectivity >= 1.0)
                    break;
                selectivity += aggregate_key_selectivity * selectivity;
            }
        }
        statistics.setOutputRowSize(selectivity * input.getOutputRowSize());
    }

    /// 3. adjust ndv
    statistics.adjustStatistics();

    /// TODO cube, grouping set, total

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
