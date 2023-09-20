#include <QueryCoordination/Optimizer/Statistics/DeriveStatistics.h>

#include <Core/Joins.h>
#include <DataTypes/DataTypeDateTime.h>
#include <QueryCoordination/Optimizer/Statistics/ExpressionStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/PredicateStatsCalculator.h>
#include <QueryCoordination/Optimizer/Statistics/IStatisticsStorage.h>
#include <QueryCoordination/Optimizer/Statistics/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    ActionsDAGPtr buildActionDAGForJoin(std::vector<TableJoin::JoinOnClause>);
};

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
    {
        row_count += input_statistics[i].getOutputRowSize(); /// TODO handle different cases.
    }

    statistics.setOutputRowSize(std::max(1.0, row_count));
    return statistics;
}

Statistics DeriveStatistics::visit(ReadFromMergeTree & step)
{

    chassert(input_statistics.empty());

    auto storage_id = step.getStorageID();
    auto cluster_name = context->getQueryCoordinationMetaInfo().cluster_name;

    /// 1. init by statistics storage
    auto input = context->getStatisticsStorage()->get(storage_id, cluster_name);
    if (!input)
    {
        input = std::make_shared<Statistics>();
    }

    Statistics statistics = *input;

    /// Final statistics output column names.
    const auto & output_columns = step.getOutputStream().header.getNames();

    /// Remove the additional columns and add missing ones.
    adjustStatisticsByColumns(statistics, output_columns);

    /// 2. calculate row count
    auto row_count = step.getAnalysisResult().selected_rows * context->getCluster(cluster_name)->getShardCount();
    statistics.setOutputRowSize(row_count);

    /// For action_dags in prewhere do not contains all output nodes,
    /// we should append other nodes to statistics
    auto append_column_stats = [&output_columns](Statistics & statistics_)
    {
        for (const auto & column : output_columns)
        {
            if (!statistics_.containsColumnStatistics(column))
                statistics_.addColumnStatistics(column, ColumnStatistics::unknown());
        }
    };

    /// 3. calculate for prewhere filters
    if (step.getPrewhereInfo())
    {
        auto prewhere_info = step.getPrewhereInfo();
        if (prewhere_info->row_level_filter)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->row_level_filter, prewhere_info->row_level_column_name, statistics, output_columns);

            statistics.removeColumnStatistics(prewhere_info->row_level_column_name);
            append_column_stats(statistics);
        }

        if (prewhere_info->prewhere_actions)
        {
            statistics = PredicateStatsCalculator::calculateStatistics(
                prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name, statistics, output_columns);

            if (prewhere_info->remove_prewhere_column)
                statistics.removeColumnStatistics(prewhere_info->prewhere_column_name);
            append_column_stats(statistics);
        }
    }

    /// 4. calculate for pushed down filters
    for (size_t i = 0; i < step.getFilters().size(); i++)
    {
        auto & predicate = step.getFilters()[i];
        auto & predicate_node_name = step.getFilterNodes().nodes[i]->result_name;
        statistics = PredicateStatsCalculator::calculateStatistics(predicate, predicate_node_name, statistics, output_columns);
    }

    statistics.adjustStatistics();
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

    /// 1. initialize statistics
    chassert(input_names.size() == output_names.size());
    for (size_t i = 0; i < input_names.size(); i++)
    {
        /// The input name and output name are one-to-one correspondences.
        auto column_stats = input.getColumnStatistics(input_names[i]);
        statistics.addColumnStatistics(output_names[i], column_stats->clone());
    }

    /// 2. calculate selectivity
    Float64 selectivity;
    const auto & aggregate_keys = step.getParams().keys;

    if (statistics.hasUnknownColumn())
    {
        /// Estimate by multiplying some coefficient
        selectivity = 0.1; /// TODO add to settings
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
        chassert(input_statistics.front().containsColumnStatistics(output_column));

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
    chassert(input_statistics.size() == 2);

    /// 1. calculate cross join statistics
    auto & left_stats = input_statistics[0];
    auto & right_stats = input_statistics[1];

    statistics.addAllColumnsFrom(left_stats);
    statistics.addAllColumnsFrom(right_stats);

    /// 2. filter by on_expression

    auto cross_join_row_count = input_statistics[0].getOutputRowSize() * input_statistics[1].getOutputRowSize();
    Float64 row_count = cross_join_row_count;

    const auto & join = step.getJoin()->getTableJoin();
    auto join_kind = join.getTableJoin().kind;

    /// whether on clause has unknown column
    bool has_unknown_column = false;
    if (join_kind != JoinKind::Cross)
    {
        const auto & on_clauses = join.getClauses();
        LOG_DEBUG(log, "Has {} on clause for query.", on_clauses.size());

        for (auto & on_clause : on_clauses)
        {
            if (statistics.hasUnknownColumn(on_clause.key_names_left))
                has_unknown_column = true;

            if (statistics.hasUnknownColumn(on_clause.key_names_right))
                has_unknown_column = true;
        }

        if (has_unknown_column)
        {
            Float64 selectivity = 1.0;
            for (size_t i = 0; i < on_clauses.size(); i++)
            {
                selectivity *= 0.1; /// TODO add to settings
            }
            row_count = cross_join_row_count * selectivity;
        }
        else
        {
            /// TODO calculate by overlap ratio of [min, max] and ndv
            Float64 selectivity = 1.0;
            for (size_t i = 0; i < on_clauses.size(); i++)
            {
                selectivity *= 0.1; /// TODO add to settings
            }
            row_count = cross_join_row_count * selectivity;
        }

        auto strictness = join.getTableJoin().strictness;
        switch (join_kind)
        {
            case JoinKind::Left:
                if (strictness == JoinStrictness::Any || strictness == JoinStrictness::Semi)
                row_count = left_stats.getOutputRowSize();
                break;
            case JoinKind::Right:
                row_count = right_stats.getOutputRowSize();
                break;
            case JoinKind::Inner:
                row_count = std::min(left_stats.getOutputRowSize(), right_stats.getOutputRowSize());
                break;
            case JoinKind::Full:
                row_count = std::max(left_stats.getOutputRowSize(), right_stats.getOutputRowSize());
                break;
            default:
                row_count = cross_join_row_count;
        }
        row_count = std::max(1.0, row_count);

    }

    /// 3. filter by constant on_expression, such as: 't1 join t2 on 1'
    if (isAlwaysFalse(join.getTableJoin().on_expression))
        row_count = 1.0;

    /// 4. calculate join strictness
    if (join.getTableJoin().strictness == JoinStrictness::Anti)
        row_count = cross_join_row_count - row_count;

    /// 5. calculate on_expression columns statistics

    for (const auto & on_clause : join.getClauses())
    {
        chassert(on_clause.key_names_left.size() == on_clause.key_names_right.size());
        for (size_t i = 0; i < on_clause.key_names_left.size(); i++)  ///  TODO inner join with equal
        {
            const auto & left_key = on_clause.key_names_left[i];
            const auto & right_key = on_clause.key_names_right[i];

            auto output_columns = step.getOutputStream().header.getNames();

            bool left_key_in_output_column = std::find(output_columns.begin(), output_columns.end(), left_key) != output_columns.end();
            bool right_key_in_output_column = std::find(output_columns.begin(), output_columns.end(), right_key) != output_columns.end();

            auto left_key_stats = left_stats.getColumnStatistics(left_key);
            auto right_key_stats = right_stats.getColumnStatistics(right_key);

            if (left_key_in_output_column && !right_key_in_output_column)
            {
                left_key_stats->mergeColumnByIntersect(right_key_stats);
                statistics.removeColumnStatistics(right_key);
            }
            else if (!left_key_in_output_column && right_key_in_output_column)
            {
                right_key_stats->mergeColumnByIntersect(left_key_stats);
                statistics.removeColumnStatistics(left_key);
            }
        }
    }

    /// 6. remove non output column statistics

    auto stats_columns = statistics.getColumnNames();
    auto output_columns = step.getOutputStream().header.getNames();

    if (stats_columns.size() > output_columns.size())
    {
        std::sort(stats_columns.begin(), stats_columns.end());
        std::sort(output_columns.begin(), output_columns.end());

        Names non_output_columns;
        std::set_difference(
            stats_columns.begin(), stats_columns.end(), output_columns.begin(), output_columns.end(), non_output_columns.begin());

        for (const auto & non_output_column : non_output_columns)
        {
            statistics.removeColumnStatistics(non_output_column);
        }
    }

    statistics.setOutputRowSize(std::max(1.0, row_count));
    statistics.adjustStatistics();

    return statistics;
}

Statistics DeriveStatistics::visit(UnionStep & step)
{
    Statistics statistics;

    chassert(input_statistics.size() == 2);
    chassert(step.getInputStreams().size() == 2);

    auto output_columns = step.getOutputStream().header.getNames();

    for (size_t i = 0; i < input_statistics.size(); i++)
    {
        chassert(step.getInputStreams()[i].header.getNames().size() == output_columns.size());
        chassert(input_statistics[i].getColumnStatisticsSize() == output_columns.size());
    }

    /// init by the first input
    auto first_input_columns = step.getInputStreams()[0].header.getNames();
    auto & first_stats = input_statistics[0];

    for (size_t i = 0; i < output_columns.size(); i++)
    {
        auto column_stats = first_stats.getColumnStatistics(first_input_columns[i]);
        statistics.addColumnStatistics(output_columns[i], column_stats);
    }

    /// merge the second input
    auto second_input_columns = step.getInputStreams()[1].header.getNames();
    auto & second_stats = input_statistics[1];

    for (size_t i = 0; i < output_columns.size(); i++)
    {
        auto column_stats = second_stats.getColumnStatistics(second_input_columns[i]);
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

Statistics DeriveStatistics::visit(TopNStep & step)
{
    chassert(input_statistics.size() == 1);
    Statistics statistics = input_statistics.front().clone();

    size_t length = step.getLimitForSorting();
    if (length)
    {
        Float64 input_row_size = input_statistics.front().getOutputRowSize();
        if (length < input_row_size)
            statistics.setOutputRowSize(length);
    }

    statistics.adjustStatistics();
    return statistics;
}

}
