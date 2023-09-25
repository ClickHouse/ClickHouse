#include <QueryCoordination/Optimizer/Statistics/JoinStatsCalculator.h>

#include <QueryCoordination/Optimizer/Statistics/Utils.h>

namespace DB
{

Statistics JoinStatsCalculator::calculateStatistics(JoinStep & step, const Statistics & left_input, const Statistics & right_input)
{
    return Calculator(step, left_input, right_input).calculate();
}

Statistics JoinStatsCalculator::Calculator::calculate()
{
    Statistics statistics;

    /// initialize statistics
    statistics.addAllColumnsFrom(left_input);
    statistics.addAllColumnsFrom(right_input);

    const auto & join = step.getJoin()->getTableJoin();
    switch (join.getTableJoin().kind)
    {
        case JoinKind::Cross:
        case JoinKind::Comma:
            calculateCrossJoin(statistics);
            break;
        case JoinKind::Inner:
            calculateInnerJoin(statistics);
            break;
        case JoinKind::Left:
        case JoinKind::Right:
        case JoinKind::Full:
            calculateOuterJoin(statistics);
            break;
    }

    /// 2. filter by on_expression

        auto cross_join_row_count = left_input.getOutputRowSize() * right_input.getOutputRowSize();
        Float64 row_count = cross_join_row_count;

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

            Float64 intersect_row_count = 1.0;

            //        /// row count after filtering left by right
            //        Float64 filter_left_row_count = 1.0;
            //        /// row count after filtering right by left
            //        Float64 filter_right_row_count = 1.0;

            auto strictness = join.getTableJoin().strictness;
            switch (join_kind)
            {
                case JoinKind::Left:
                    if (strictness == JoinStrictness::Any)
                        row_count = left_input.getOutputRowSize();
                    else if (strictness == JoinStrictness::Semi)
                        row_count = intersect_row_count;
                    else if (strictness == JoinStrictness::Semi)
                        row_count = left_input.getOutputRowSize() - intersect_row_count;
                    break;
                case JoinKind::Right:
                    row_count = right_input.getOutputRowSize();
                    break;
                case JoinKind::Inner:
                    row_count = std::min(left_input.getOutputRowSize(), right_input.getOutputRowSize());
                    break;
                case JoinKind::Full:
                    row_count = std::max(left_input.getOutputRowSize(), right_input.getOutputRowSize());
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

                auto left_key_stats = left_input.getColumnStatistics(left_key);
                auto right_key_stats = right_input.getColumnStatistics(right_key);

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

    /// remove non output column statistics
    removeNonOutputColumn(statistics);

    statistics.adjustStatistics();
    return statistics;
}

void JoinStatsCalculator::Calculator::calculateCrossJoin(Statistics & input)
{
    input.setOutputRowSize(left_input.getOutputRowSize() * right_input.getOutputRowSize());
}

bool JoinStatsCalculator::Calculator::hasUnknownStatsColumn(const std::vector<TableJoin::JoinOnClause> on_clauses)
{
    bool has_unknown_column = false;

    for (auto & on_clause : on_clauses)
    {
        if (left_input.hasUnknownColumn(on_clause.key_names_left))
            has_unknown_column = true;

        if (right_input.hasUnknownColumn(on_clause.key_names_right))
            has_unknown_column = true;
    }
    return has_unknown_column;
}

void JoinStatsCalculator::Calculator::calculateInnerJoin(Statistics & input)
{
    const auto & join = step.getJoin()->getTableJoin();
    const auto & on_clauses = join.getClauses();

    LOG_DEBUG(log, "Has {} on clause for query.", on_clauses.size());

    /// whether join on key has columns whose statistics is unknown
    bool has_unknown_column = hasUnknownStatsColumn(on_clauses);

    if (has_unknown_column)
    {
        Float64 selectivity = 1.0;
        for (size_t i = 0; i < on_clauses.size(); i++)
        {
            selectivity *= 0.1; /// TODO add to settings
        }
        row_count = cross_join_row_count * selectivity;
    }
}

void JoinStatsCalculator::Calculator::calculateOuterJoin(Statistics &)
{
}

void JoinStatsCalculator::Calculator::calculateFilterJoin(Statistics &)
{
}

void JoinStatsCalculator::Calculator::calculateAnyJoin(Statistics &)
{
}

void JoinStatsCalculator::Calculator::removeNonOutputColumn(Statistics & input)
{
    auto stats_columns = input.getColumnNames();
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
            input.removeColumnStatistics(non_output_column);
        }
    }
}

}
