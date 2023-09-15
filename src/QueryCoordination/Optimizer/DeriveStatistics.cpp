#include <DataTypes/DataTypeDateTime.h>
#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB
{

Statistics DeriveStatistics::visit(QueryPlanStepPtr step)
{
    LOG_TRACE(log, "collect statistics for step {}", step->getName());
    return Base::visit(step);
}

Statistics DeriveStatistics::visitDefault(IQueryPlanStep & step)
{
    auto output_columns = step.getOutputStream().header.getNames();

    /// Return unknown statistics for non merge tree family storage engine.
    if (typeid_cast<ISourceStep *>(&step))
    {
        return Statistics::unknown(output_columns);
    }

    Statistics statistics;
    chassert(input_statistics.size() == step.getInputStreams().size());

    /// Just find in input statistics by output column name,
    // if not found (step may change input columns) make unknown column statistics
    for (const auto & output_column : output_columns)
    {
        ColumnStatisticsPtr output_column_stats;
        for (size_t i = 0; i < input_statistics.size(); i++)
        {
            output_column_stats = input_statistics[i].getColumnStatistics(output_column);
            if (output_column_stats)
                break;
        }
        if (output_column_stats)
            statistics.addColumnStatistics(output_column, output_column_stats);
        else
            statistics.addColumnStatistics(output_column, ColumnStatistics::unknown());
    }

    /// Calculate output row count
    Float64 row_count = 0.0;
    for (size_t i = 0; i < input_statistics.size(); i++)
    {
        row_count += input_statistics[i].getOutputRowSize(); /// TODO handle different cases.
    }

    statistics.setOutputRowSize(std::max(1.0, row_count));
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
    for (size_t i = 0; i < step.getFilters().size(); i++)
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
    Float64 selectivity;
    const auto & aggregate_keys = step.getParams().keys;

    if (statistics.hasUnknownColumn())
    {
        /// Estimate by multiplying some coefficient
        selectivity = 0.2; /// TODO add to settings
        for (size_t i = 1; i < aggregate_keys.size(); i++)
        {
            if (selectivity * 1.1 > 1.0)
                break;
            selectivity *= 1.1; /// TODO add to settings
        }
    }
    else
    {
        /// Estimate by ndv
        selectivity = 1.0;
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
    }

    /// The selectivity calculated is global, but for the first stage,
    /// the output row count is larger than the final stage.
    if (!step.isFinal())
        selectivity *= 5; /// TODO add to settings

    statistics.setOutputRowSize(selectivity * input.getOutputRowSize());

    /// 3. adjust ndv
    statistics.adjustStatistics();

    return statistics;
}

Statistics DeriveStatistics::visit(MergingAggregatedStep & step)
{
    for (auto & output_column : step.getOutputStream().header.getNames())
        chassert(input_statistics.front().getColumnStatistics(output_column));

    Statistics statistics = input_statistics.front().clone();
    statistics.setOutputRowSize(statistics.getOutputRowSize() / 5);

    return statistics;
}

Statistics DeriveStatistics::visit(SortingStep & step)
{
    return visitDefault(step);
}


Statistics DeriveStatistics::visit(LimitStep & step)
{
    chassert(input_statistics.size() == 1);
    Statistics statistics = input_statistics.front().clone();

    if (step.getLimit())
    {
        Float64 input_row_size = input_statistics.front().getOutputRowSize();
        size_t length = step.getOffset() + step.getLimit();
        if (length < input_row_size)
            statistics.setOutputRowSize(length);
    }

    statistics.adjustStatistics();
    return statistics;
}

Statistics DeriveStatistics::visit(JoinStep & step)
{
    Statistics statistics;
    for (auto & output_column : step.getOutputStream().header.getNames())
    {
        ColumnStatisticsPtr output_column_stats;
        for (auto & input_stats : input_statistics)
        {
            if (input_stats.getColumnStatistics(output_column))
            {
                output_column_stats = input_stats.getColumnStatistics(output_column)->clone();
                break;
            }
        }
        chassert(output_column_stats);
        statistics.addColumnStatistics(output_column, output_column_stats);
    }

    /// TODO Join type inner cross anti-...
    /// TODO on predicate
//    auto join = step.getJoin();
//    auto & left_stream = step.getInputStreams()[0];
//    auto & right_stream = step.getInputStreams()[1];
//
    auto & left_stats = input_statistics[0];
    auto & right_stats = input_statistics[1];

    Float64 row_count = (left_stats.getOutputRowSize() + right_stats.getOutputRowSize()) * 0.1;
    statistics.setOutputRowSize(std::max(1.0, row_count));

    return statistics;
}

Statistics DeriveStatistics::visit(UnionStep & step)
{
    Statistics statistics;

    chassert(input_statistics.size() == 2);
    chassert(step.getInputStreams().size() == 2);

    auto output_columns = step.getOutputStream().header.getNames();

    for (size_t i = 0; input_statistics.size(); i++)
    {
        chassert(step.getInputStreams()[i].header.getNames().size() == output_columns.size());
        chassert(input_statistics[i].getColumnStatisticsMap().size() == output_columns.size());
    }

    /// init by the first input
    auto first_input_columns = step.getInputStreams()[0].header.getNames();
    auto & first_stats = input_statistics[0];

    for (size_t i = 0; output_columns.size(); i++)
    {
        auto column_stats = first_stats.getColumnStatistics(first_input_columns[i]);
        chassert(column_stats);
        statistics.addColumnStatistics(output_columns[i], column_stats);
    }

    /// merge the second input
    auto second_input_columns = step.getInputStreams()[1].header.getNames();
    auto & second_stats = input_statistics[1];

    for (size_t i = 0; output_columns.size(); i++)
    {
        auto column_stats = second_stats.getColumnStatistics(second_input_columns[i]);
        chassert(column_stats);
        statistics.mergeColumnByUnion(output_columns[i], column_stats);
    }

    /// calculate output row size;
    Float64 output_row_size = 0;
    for (const auto & input_statistic : input_statistics)
    {
        output_row_size += input_statistic.getOutputRowSize();
    }
    statistics.setOutputRowSize(output_row_size);

    return statistics;
}

Statistics DeriveStatistics::visit(ExchangeDataStep & /*step*/)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Should never reach here, for the statistics of group of ExchangeDataStep is calculated by other step."
        "And we just skip the calculating the step.");
}

}
