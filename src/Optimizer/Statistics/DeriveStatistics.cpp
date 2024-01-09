#include <Optimizer/Statistics/DeriveStatistics.h>

#include <DataTypes/DataTypeDateTime.h>
#include <Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <Optimizer/Statistics/IStatisticsStorage.h>
#include <Optimizer/Statistics/JoinStatsCalculator.h>
#include <Optimizer/Statistics/PredicateStatsCalculator.h>
#include <Optimizer/Statistics/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Stats DeriveStatistics::visit(QueryPlanStepPtr step)
{
    LOG_TRACE(log, "Collecting statistics for step {}", step->getName());
    return Base::visit(step);
}

Stats DeriveStatistics::visitDefault(IQueryPlanStep & step)
{
    auto output_columns = step.getOutputStream().header.getNames();

    /// Return unknown statistics for non merge tree family storage engine.
    if (typeid_cast<ISourceStep *>(&step))
        return Stats::unknown(output_columns);

    Stats statistics;
    chassert(input_statistics.size() == step.getInputStreams().size());

    /// Just find in input statistics by output column name,
    // if not found (step may change input columns) make unknown column statistics
    for (const auto & output_column : output_columns)
    {
        ColumnStatisticsPtr output_column_stats;
        for (size_t i = 0; i < input_statistics.size(); i++)
        {
            if (input_statistics[i].containsColumnStatistics(output_column))
            {
                output_column_stats = input_statistics[i].getColumnStatistics(output_column);
                break;
            }
        }
        if (output_column_stats)
            statistics.addColumnStatistics(output_column, output_column_stats->clone());
        else
            statistics.addColumnStatistics(output_column, ColumnStatistics::unknown());
    }

    /// Calculate output row count
    Float64 row_count = 0.0;
    for (size_t i = 0; i < input_statistics.size(); i++)
        row_count += input_statistics[i].getOutputRowSize(); /// TODO handle different cases.

    statistics.setOutputRowSize(std::max(1.0, row_count));
    return statistics;
}

Stats DeriveStatistics::visit(ReadFromMergeTree & step)
{
    chassert(input_statistics.empty());

    auto storage_id = step.getStorageID();
    auto cluster_name = context->getQueryCoordinationMetaInfo().cluster_name;

    /// 1. init by statistics storage
    auto input = context->getStatisticsStorage()->get(storage_id, cluster_name);
    if (!input)
        input = std::make_shared<Stats>();

    /// Final statistics output column names.
    const auto & output_columns = step.getOutputStream().header.getNames();

    /// Add all columns to statistics
    auto add_column_if_not_exist = [&input](const Names & columns)
    {
        for (const auto & column : columns)
            if (!input->containsColumnStatistics(column))
                input->addColumnStatistics(column, ColumnStatistics::unknown());
    };

    add_column_if_not_exist(step.getRealColumnNames());
    add_column_if_not_exist(step.getVirtualColumnNames());

    /// Firstly we set table total row count as table total row count, and
    /// when driving statistics for filter step the row count will reduce.
    /// TODO Driving statistics for filter step should support data type String
    /// whose value can be cast to Float64.
    Stats statistics = *input;

    /// For action_dags in prewhere do not contains all output nodes,
    /// we should append other nodes to statistics. For example: SELECT a, b from t where a > 1;
    auto append_column_stats = [&input, &statistics]()
    {
        for (const auto & column : input->getColumnNames())
            if (!statistics.containsColumnStatistics(column))
                statistics.addColumnStatistics(column, input->getColumnStatistics(column)->clone());
    };

    /// 2. calculate for prewhere filters
    if (step.getPrewhereInfo())
    {
        const auto & prewhere_info = step.getPrewhereInfo();
        if (prewhere_info->row_level_filter)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->row_level_filter, prewhere_info->row_level_column_name, statistics);

            statistics.removeColumnStatistics(prewhere_info->row_level_column_name);
            append_column_stats();
        }

        if (prewhere_info->prewhere_actions)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name, statistics);

            if (prewhere_info->remove_prewhere_column)
                statistics.removeColumnStatistics(prewhere_info->prewhere_column_name);
            append_column_stats();
        }
    }

    /// 3. calculate for pushed down filters
    for (size_t i = 0; i < step.getFilters().size(); i++)
    {
        const auto & predicate = step.getFilters()[i];
        const auto & predicate_node_name = step.getFilterNodes().nodes[i]->result_name;
        statistics = PredicateStatsCalculator::calculateStatistics(predicate, predicate_node_name, statistics);
        append_column_stats();
    }

    /// Remove the additional columns and add missing ones.
    adjustStatisticsByColumns(statistics, output_columns);

    statistics.adjustStatistics();
    return statistics;
}

Stats DeriveStatistics::visit(ExpressionStep & step)
{
    chassert(input_statistics.size() == 1);
    const auto & action_dag = step.getExpression();
    Stats statistics = ExpressionStatsCalculator::calculateStatistics(action_dag, input_statistics.front());
    return statistics;
}

Stats DeriveStatistics::visit(FilterStep & step)
{
    chassert(input_statistics.size() == 1);
    Stats statistics
        = PredicateStatsCalculator::calculateStatistics(step.getExpression(), step.getFilterColumnName(), input_statistics.front());
    return statistics;
}

Stats DeriveStatistics::visit(AggregatingStep & step)
{
    Stats statistics;

    Names input_names = step.getInputStreams().front().header.getNames();
    Names output_names = step.getOutputStream().header.getNames();

    const auto & input = input_statistics.front();

    /// 1. initialize statistics

    /// The first column of grouping set aggregation is "__grouping_set"
    if (step.isGroupingSets())
        statistics.addColumnStatistics(output_names[0], ColumnStatistics::create(1.0));

    /// keys
    for (const String & key : step.getParams().keys)
    {
        auto column_stats = input.getColumnStatistics(key);
        statistics.addColumnStatistics(key, column_stats->clone());
    }

    /// aggregates
    for (const auto & aggregate : step.getParams().aggregates)
    {
        statistics.addColumnStatistics(aggregate.column_name, ColumnStatistics::create(1.0));
    }

    /// 2. calculate selectivity
    Float64 selectivity;
    const auto & aggregate_keys = step.getParams().keys;
    if (aggregate_keys.empty())
    {
        selectivity = 0.0;
    }
    else if (statistics.hasUnknownColumn())
    {
        auto first_key_coefficient = stats_settings.statistics_agg_unknown_column_first_key_coefficient;
        auto rest_key_coefficient = stats_settings.statistics_agg_unknown_column_first_key_coefficient;

        /// Estimate by multiplying a coefficient
        selectivity = first_key_coefficient;
        for (size_t i = 1; i < aggregate_keys.size(); i++)
        {
            if (selectivity * rest_key_coefficient > 1.0)
                break;
            selectivity *= rest_key_coefficient;
        }
    }
    else
    {
        /// Estimate by ndv
        selectivity = 1.0;
        for (size_t i = 0; i < aggregate_keys.size(); i++)
        {
            auto aggregate_key_stats = statistics.getColumnStatistics(aggregate_keys[i]);

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
    if (step.isPreliminaryAgg())
    {
        /// Final selectivity calculation formula:
        ///     selectivity *= node_count * coefficient.
        /// First we assume data is evenly distributed into shards and all shards has full
        /// cardinality of data set. But in practice a shard may have only partial of cardinality,
        /// so we multiply a coefficient.

        //        /// The coefficient is effected by selectivity, because
        //        /// the lower of the selectivity the more chance of shard has full cardinality.
        //        /// The coefficient is calculated based on a mapping of selectivity and coefficients
        //        /// which the value is from practice.
        //        static std::vector<std::pair<Float64, Float64>> mapping
        //            = {{0.0, 0.0}, {0.001, 0.1}, {0.01, 0.2}, {0.1, 0.5}, {0.5, 0.7}, {1.0, 1.0}};
        //        auto get_coefficient = [](Float64 selectivity_)
        //        {
        //            if (selectivity_ <= mapping.front().first)
        //                return mapping.front().second;
        //
        //            if (selectivity_ >= mapping.back().first)
        //                return mapping.back().second;
        //
        //            for (size_t i = 0; i < mapping.size() - 1; i++)
        //            {
        //                if (selectivity_ >= mapping[i].first && selectivity_ < mapping[i + 1].first)
        //                {
        //                    Float64 x1 = mapping[i].first;
        //                    Float64 y1 = mapping[i].second;
        //                    Float64 x2 = mapping[i + 1].first;
        //                    Float64 y2 = mapping[i + 1].second;
        //                    Float64 coefficient = y1 + (y2 - y1) * (selectivity_ - x1) / (x2 - x1);
        //                    return coefficient;
        //                }
        //            }
        //            return 0.0; // Default coefficient if there are no valid mappings
        //        };
        //        selectivity = selectivity * node_count * get_coefficient(selectivity);
        auto coefficient = stats_settings.statistics_agg_full_cardinality_coefficient;
        selectivity = selectivity * node_count * coefficient;
    }

    if (selectivity > 1.0)
        selectivity = 1.0;

    statistics.setOutputRowSize(selectivity * input.getOutputRowSize());

    /// 3. adjust ndv
    statistics.adjustStatistics();

    /// 4. update aggregating column data type and row size
    for (const auto & aggregate : step.getParams().aggregates)
    {
        const auto * output_column = step.getOutputStream().header.findByName(aggregate.column_name);

        /// Input stream of aggregating step may has 0 header column, such as: 'select count() from t'.
        if (!statistics.containsColumnStatistics(aggregate.column_name))
            statistics.addColumnStatistics(aggregate.column_name, ColumnStatistics::unknown());

        auto output_column_stats = statistics.getColumnStatistics(aggregate.column_name);
        chassert(output_column && output_column_stats);

        output_column_stats->setDataType(output_column->type);

        /// For uniq and uniqExact usually has large stat, so we should update the row size.
        if (aggregate.function->getName() == "uniq" || aggregate.function->getName() == "uniqExact")
            output_column_stats->setAvgRowSize(
                input.getOutputRowSize() * output_column_stats->getAvgRowSize() / statistics.getOutputRowSize());
    }

    return statistics;
}

Stats DeriveStatistics::visit(MergingAggregatedStep & step)
{
    for (auto & output_column : step.getOutputStream().header.getNames())
        chassert(input_statistics.front().containsColumnStatistics(output_column));

    Stats statistics = input_statistics.front().clone();
    Float64 row_count;

    /// The secondary stage of aggregating
    auto coefficient = stats_settings.statistics_agg_full_cardinality_coefficient;
    row_count = statistics.getOutputRowSize() / node_count / coefficient;

    statistics.setOutputRowSize(row_count);
    return statistics;
}

Stats DeriveStatistics::visit(SortingStep & step)
{
    return visitDefault(step);
}

Stats DeriveStatistics::visit(CreatingSetsStep & step)
{
    auto output_columns = step.getOutputStream().header.getNames();

    Stats statistics;
    chassert(input_statistics.size() == step.getInputStreams().size());

    /// Just find in input statistics by output column name,
    // if not found (step may change input columns) make unknown column statistics
    for (const auto & output_column : output_columns)
    {
        ColumnStatisticsPtr output_column_stats;
        for (size_t i = 0; i < input_statistics.size(); i++)
        {
            if (input_statistics[i].containsColumnStatistics(output_column))
            {
                output_column_stats = input_statistics[i].getColumnStatistics(output_column);
                break;
            }
        }
        if (output_column_stats)
            statistics.addColumnStatistics(output_column, output_column_stats->clone());
        else
            statistics.addColumnStatistics(output_column, ColumnStatistics::unknown());
    }

    /// Calculate output row count
    Float64 row_count = 0.0;
    for (const auto & input_statistic : input_statistics)
        row_count += input_statistic.getOutputRowSize(); /// TODO handle different cases.

    statistics.setOutputRowSize(std::max(1.0, row_count));
    return statistics;
}

Stats DeriveStatistics::visit(LimitStep & step)
{
    chassert(input_statistics.size() == 1);
    Stats statistics = input_statistics.front().clone();

    Float64 row_count = statistics.getOutputRowSize();

    if (step.getLimit())
    {
        size_t length = step.getOffset() + step.getLimit();
        /// Two stage limiting, first stage
        if (step.getPhase() == LimitStep::Phase::Preliminary)
        {
            if (length < row_count)
                row_count = length;
            row_count = row_count * node_count;
        }
        /// Two stage limiting, second stage
        else if (step.getPhase() == LimitStep::Phase::Final)
        {
            if (length < row_count)
                row_count = length;
        }
        /// Single stage limiting
        else
        {
            if (length < row_count)
                row_count = length;
        }
    }

    statistics.setOutputRowSize(row_count);
    statistics.adjustStatistics();

    return statistics;
}

Stats DeriveStatistics::visit(JoinStep & step)
{
    Stats statistics;
    chassert(input_statistics.size() == 2);
    return JoinStatsCalculator::calculateStatistics(step, input_statistics[0], input_statistics[1]);
}

Stats DeriveStatistics::visit(UnionStep & step)
{
    Stats statistics;

    chassert(input_statistics.size() > 1);
    chassert(step.getInputStreams().size() == input_statistics.size());

    auto output_columns = step.getOutputStream().header.getNames();

    for (size_t i = 0; i < input_statistics.size(); i++)
    {
        chassert(step.getInputStreams()[i].header.getNames().size() == output_columns.size());
        chassert(input_statistics[i].getColumnStatisticsSize() == output_columns.size());
    }

    /// init by the first input
    auto first_input_columns = step.getInputStreams()[0].header.getNames();
    const auto & first_stats = input_statistics[0];

    for (size_t i = 0; i < output_columns.size(); i++)
    {
        auto column_stats = first_stats.getColumnStatistics(first_input_columns[i]);
        statistics.addColumnStatistics(output_columns[i], column_stats);
    }

    /// merge the left inputs
    for (size_t i = 1; i < step.getInputStreams().size(); i++)
    {
        auto left_input_columns = step.getInputStreams()[i].header.getNames();
        auto & left_stats = input_statistics[i];

        for (size_t j = 0; j < output_columns.size(); j++)
        {
            auto column_stats = left_stats.getColumnStatistics(left_input_columns[j]);
            auto output_column_stats = statistics.getColumnStatistics(output_columns[j]);

            /// merge min_value / max_value
            output_column_stats->mergeColumnValueByUnion(column_stats);

            /// merge ndv
            auto ndv = std::max(column_stats->getNdv(), output_column_stats->getNdv());
            ndv = ndv + (ndv - std::min(column_stats->getNdv(), output_column_stats->getNdv())) * 0.1; /// TODO add to settings
            output_column_stats->setNdv(ndv);
        }
    }

    /// calculate output row size;
    Float64 output_row_size = 0;
    for (const auto & input_statistic : input_statistics)
        output_row_size += input_statistic.getOutputRowSize();
    statistics.setOutputRowSize(output_row_size);

    return statistics;
}

Stats DeriveStatistics::visit(ExchangeDataStep & /*step*/)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Should never reach here, for the statistics of group of ExchangeDataStep is calculated by other step."
        "And we just skip the calculating the step.");
}

Stats DeriveStatistics::visit(TopNStep & step)
{
    chassert(input_statistics.size() == 1);
    Stats statistics = input_statistics.front().clone();

    Float64 row_count = statistics.getOutputRowSize();

    size_t length = step.getLimitForSorting();
    if (length)
    {
        /// Two stage topn, first stage
        if (step.getPhase() == TopNStep::Phase::Preliminary)
        {
            if (length < row_count)
                row_count = length;
            row_count = row_count * node_count;
        }
        /// Two stage topn, second stage
        else if (step.getPhase() == TopNStep::Phase::Final)
        {
            if (length < row_count)
                row_count = length;
        }
        else
        {
            if (length < row_count)
                row_count = length;
        }
    }

    statistics.setOutputRowSize(row_count);
    statistics.adjustStatistics();

    return statistics;
}

}
