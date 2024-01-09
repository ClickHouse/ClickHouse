#include <cmath>
#include <Optimizer/Statistics/JoinStatsCalculator.h>
#include <Optimizer/Statistics/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

Stats JoinStatsCalculator::calculateStatistics(JoinStep & step, const Stats & left_input, const Stats & right_input)
{
    return Impl(step, left_input, right_input).calculate();
}

JoinStatsCalculator::Impl::Impl(JoinStep & step_, const Stats & left_input_, const Stats & right_input_)
    : step(step_), left_input(left_input_), right_input(right_input_)
{
    const auto & join = step.getJoin()->getTableJoin();
    auto & on_clauses = join.getClauses();

    for (auto & on_clause : on_clauses)
    {
        for (auto & left_key : on_clause.key_names_left)
            left_join_on_keys.push_back(left_key);
        for (auto & right_key : on_clause.key_names_right)
            right_join_on_keys.push_back(right_key);
    }

    output_columns = step.getOutputStream().header.getNames();
}

Stats JoinStatsCalculator::Impl::calculate()
{
    Stats statistics;

    /// initialize statistics
    statistics.addAllColumnsFrom(left_input);
    statistics.addAllColumnsFrom(right_input);

    const auto & join = step.getJoin()->getTableJoin();
    switch (join.getTableJoin().kind)
    {
        case JoinKind::Cross:
        case JoinKind::Comma:
        case JoinKind::Paste:
            calculateCrossJoin(statistics);
            break;
        case JoinKind::Inner:
            calculateInnerJoin(statistics, join.strictness());
            break;
        case JoinKind::Left:
        case JoinKind::Right:
        case JoinKind::Full:
            calculateOuterJoin(statistics, join.strictness());
            break;
    }

    /// filter by constant on_expression, such as: 't1 join t2 on 1'
    if (join.getTableJoin().on_expression && isAlwaysFalse(join.getTableJoin().on_expression))
        statistics.setOutputRowSize(1.0);

    /// remove non output columns
    removeNonOutputColumn(statistics);

    /// ndv can not large than row_count
    statistics.adjustStatistics();

    return statistics;
}

void JoinStatsCalculator::Impl::calculateCrossJoin(Stats & statistics)
{
    auto row_count = left_input.getOutputRowSize() * right_input.getOutputRowSize();
    statistics.setOutputRowSize(row_count);
}

void JoinStatsCalculator::Impl::calculateAsofJoin(Stats & statistics)
{
    statistics.reset();

    auto row_count = left_input.getOutputRowSize() * right_input.getOutputRowSize(); ///TODO coefficient
    statistics.setOutputRowSize(row_count);

    for (auto & column : output_columns)
        statistics.addColumnStatistics(column, ColumnStatistics::unknown());
}

void JoinStatsCalculator::Impl::calculateInnerJoin(Stats & statistics, JoinStrictness strictness)
{
    switch (strictness)
    {
        case JoinStrictness::Unspecified:
        case JoinStrictness::All:
            calculateCommonInnerJoin(statistics, false);
            break;
        case JoinStrictness::Any:
        case JoinStrictness::RightAny:
            calculateCommonInnerJoin(statistics, true);
            break;
        case JoinStrictness::Asof:
            calculateAsofJoin(statistics);
            break;
        default:
            /// should never reach here
            break;
    }
}

void JoinStatsCalculator::Impl::calculateCommonInnerJoin(Stats & statistics, bool is_any)
{
    /// calculate row count
    Float64 row_count = calculateRowCountForInnerJoin(is_any);
    statistics.setOutputRowSize(row_count);

    /// column statistics
    calculateColumnStatsForInnerJoin(statistics);
}

void JoinStatsCalculator::Impl::calculateOuterJoin(Stats & statistics, JoinStrictness strictness)
{
    switch (strictness)
    {
        case JoinStrictness::Unspecified:
        case JoinStrictness::All:
        case JoinStrictness::RightAny:
            calculateCommonOuterJoin(statistics, false);
            break;
        case JoinStrictness::Any:
            calculateCommonOuterJoin(statistics, true);
            break;
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
            calculateFilterOuterJoin(statistics, strictness == JoinStrictness::Semi);
            break;
        case JoinStrictness::Asof:
            calculateAsofJoin(statistics);
            break;
    }
}

void JoinStatsCalculator::Impl::calculateCommonOuterJoin(Stats & statistics, bool is_any)
{
    /// calculate row count
    Float64 row_count;
    auto type = step.getJoin()->getTableJoin().kind();

    if (is_any)
    {
        switch (type)
        {
            case JoinKind::Left:
                row_count = leftDataSetNDV();
                break;
            case JoinKind::Right:
                row_count = rightDataSetNDV();
                break;
            case JoinKind::Full:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Full any join is not implemented.");
            default:
                /// should never reach here
                row_count = 1.0;
                break;
        }
    }
    else
    {
        auto inner_join_row_count = calculateRowCountForInnerJoin(false);
        auto intersecting_ndv = calculateIntersectingNDV();

        auto left_intersecting = calculateIntersectingRowCountForLeft(intersecting_ndv);
        auto right_intersecting = calculateIntersectingRowCountForRight(intersecting_ndv);

        switch (type)
        {
            case JoinKind::Left:
                row_count = left_input.getOutputRowSize() - left_intersecting + inner_join_row_count;
                break;
            case JoinKind::Right:
                row_count = right_input.getOutputRowSize() - right_intersecting + inner_join_row_count;
                break;
            case JoinKind::Full:
                row_count = (left_input.getOutputRowSize() - left_intersecting) + (right_input.getOutputRowSize() - right_intersecting)
                    + inner_join_row_count;
                break;
            default:
                /// should never reach here
                row_count = 1.0;
                break;
        }
    }
    statistics.setOutputRowSize(row_count);

    /// calculate column statistics
    calculateColumnStatsForOuterJoin(statistics, type);
}

void JoinStatsCalculator::Impl::calculateFilterOuterJoin(Stats & statistics, bool is_semi)
{
    auto type = step.getJoin()->getTableJoin().kind();

    /// row count
    Float64 row_count;
    auto intersecting_ndv = calculateIntersectingNDV();

    if (type == JoinKind::Left)
    {
        auto left_intersecting_count = calculateIntersectingRowCountForLeft(intersecting_ndv);
        row_count = is_semi ? left_intersecting_count : left_input.getOutputRowSize() - left_intersecting_count;
    }
    else
    {
        auto right_intersecting_count = calculateIntersectingRowCountForRight(intersecting_ndv);
        row_count = is_semi ? right_intersecting_count : right_input.getOutputRowSize() - right_intersecting_count;
    }
    statistics.setOutputRowSize(row_count);

    /// column statistics
    calculateColumnStatsForFilterJoin(statistics, is_semi);
}

void JoinStatsCalculator::Impl::calculateColumnStatsForFilterJoin(Stats & statistics, bool is_semi)
{
    auto type = step.getJoin()->getTableJoin().kind();
    if (is_semi)
        calculateColumnStatsForIntersecting(statistics);
    else
        calculateColumnStatsForAnti(statistics, type == JoinKind::Left);
}

bool JoinStatsCalculator::Impl::hasUnknownStatsColumn()
{
    if (left_input.hasUnknownColumn(left_join_on_keys))
        return true;
    if (right_input.hasUnknownColumn(right_join_on_keys))
        return true;
    return false;
}

Float64 JoinStatsCalculator::Impl::leftDataSetNDV()
{
    Float64 left_data_set_ndv;
    if (hasUnknownStatsColumn())
    {
        left_data_set_ndv = std::log2(left_input.getOutputRowSize());
    }
    else
    {
        left_data_set_ndv = 1 * left_input.getColumnStatistics(left_join_on_keys[0])->getNdv();
        for (size_t i = 1; i < left_join_on_keys.size(); i++)
            left_data_set_ndv = left_data_set_ndv * 0.8 * left_input.getColumnStatistics(left_join_on_keys[i])->getNdv();
    }
    return std::max(1.0, left_data_set_ndv);
}

Float64 JoinStatsCalculator::Impl::rightDataSetNDV()
{
    Float64 right_data_set_ndv;
    if (hasUnknownStatsColumn())
    {
        right_data_set_ndv = std::log2(right_input.getOutputRowSize());
    }
    else
    {
        right_data_set_ndv = 1 * right_input.getColumnStatistics(right_join_on_keys[0])->getNdv();
        for (size_t i = 1; i < right_join_on_keys.size(); i++)
            right_data_set_ndv = right_data_set_ndv * 0.8 * right_input.getColumnStatistics(right_join_on_keys[i])->getNdv();
    }
    return std::max(1.0, right_data_set_ndv);
}

Float64 JoinStatsCalculator::Impl::calculateIntersectingNDV()
{
    Float64 left_data_set_ndv = leftDataSetNDV();
    Float64 right_data_set_ndv = rightDataSetNDV();

    return std::max(1.0, std::min(left_data_set_ndv, right_data_set_ndv) * 0.5);
}

Float64 JoinStatsCalculator::Impl::calculateIntersectingRowCountForLeft(Float64 intersecting_ndv)
{
    return std::max(1.0, (intersecting_ndv / leftDataSetNDV()) * left_input.getOutputRowSize());
}

Float64 JoinStatsCalculator::Impl::calculateIntersectingRowCountForRight(Float64 intersecting_ndv)
{
    return std::max(1.0, (intersecting_ndv / rightDataSetNDV()) * right_input.getOutputRowSize());
}


Float64 JoinStatsCalculator::Impl::calculateRowCountForInnerJoin(bool is_any)
{
    Float64 intersecting_ndv = calculateIntersectingNDV();
    if (!is_any)
    {
        auto left = calculateIntersectingRowCountForLeft(intersecting_ndv);
        auto right = calculateIntersectingRowCountForRight(intersecting_ndv);
        return (left * right / intersecting_ndv) * 1.2;
    }
    else
    {
        return intersecting_ndv;
    }
}

void JoinStatsCalculator::Impl::calculateColumnStatsForInnerJoin(Stats & statistics)
{
    calculateColumnStatsForIntersecting(statistics);
}

void JoinStatsCalculator::Impl::calculateColumnStatsForIntersecting(Stats & statistics)
{
    /// For join on columns
    for (size_t i = 0; i < left_join_on_keys.size(); i++)
    {
        const auto & left_key = left_join_on_keys[i];
        const auto & right_key = right_join_on_keys[i];

        const auto left_key_stats = statistics.getColumnStatistics(left_key);
        const auto right_key_stats = statistics.getColumnStatistics(right_key);

        /// calculate min_value / max_value
        left_key_stats->mergeColumnValueByIntersect(right_key_stats);
        right_key_stats->setMinValue(left_key_stats->getMinValue());
        right_key_stats->setMaxValue(left_key_stats->getMaxValue());

        /// calculate NDV
        auto left_ndv = left_key_stats->getNdv();
        auto right_ndv = right_key_stats->getNdv();

        auto ndv = std::min(left_ndv, right_ndv) * 0.8; /// TODO add to settings
        left_key_stats->setNdv(ndv);
        right_key_stats->setNdv(ndv);
    }

    /// For non join on columns
    /// Only calculate ndv, the min_value/max_values remain unchanged.
    Names non_join_on_columns;
    auto set_ndv_for_non_join_on_columns = [&non_join_on_columns, &statistics]()
    {
        for (auto & non_join_on_column : non_join_on_columns)
        {
            auto ndv = statistics.getColumnStatistics(non_join_on_column)->getNdv() * 0.8;
            statistics.getColumnStatistics(non_join_on_column)->setNdv(ndv);
        }
    };

    /// left table
    auto all_left_columns = left_input.getColumnNames();
    std::set_difference(
        all_left_columns.begin(),
        all_left_columns.end(),
        left_join_on_keys.begin(),
        left_join_on_keys.end(),
        std::inserter(non_join_on_columns, non_join_on_columns.begin()));
    set_ndv_for_non_join_on_columns();

    non_join_on_columns.clear();

    /// right table
    auto all_right_columns = right_input.getColumnNames();
    std::set_difference(
        all_right_columns.begin(),
        all_right_columns.end(),
        right_join_on_keys.begin(),
        right_join_on_keys.end(),
        std::inserter(non_join_on_columns, non_join_on_columns.begin()));
    set_ndv_for_non_join_on_columns();
}

void JoinStatsCalculator::Impl::calculateColumnStatsForAnti(Stats & statistics, bool /*is_left*/)
{
    /// For join on columns
    for (size_t i = 0; i < left_join_on_keys.size(); i++)
    {
        const auto & left_key = left_join_on_keys[i];
        const auto & right_key = right_join_on_keys[i];

        const auto left_key_stats = statistics.getColumnStatistics(left_key);
        const auto right_key_stats = statistics.getColumnStatistics(right_key);

        /// only calculate NDV, min_value/max_value not changed.
        auto left_ndv = left_key_stats->getNdv();
        auto right_ndv = right_key_stats->getNdv();

        /// ndv for intersecting
        auto ndv = std::min(left_ndv, right_ndv) * 0.8; /// TODO add to settings
        left_key_stats->setNdv(left_key_stats->getNdv() - ndv);
        right_key_stats->setNdv(right_key_stats->getNdv() - ndv);
    }

    /// For non join on columns
    /// Only calculate ndv, the min_value/max_values remain unchanged.
    Names non_join_on_columns;
    auto set_ndv_for_non_join_on_columns = [&non_join_on_columns, &statistics]()
    {
        for (auto & non_join_on_column : non_join_on_columns)
        {
            auto ndv = statistics.getColumnStatistics(non_join_on_column)->getNdv() * (1 - 0.8);
            statistics.getColumnStatistics(non_join_on_column)->setNdv(ndv);
        }
    };

    /// left table
    auto all_left_columns = left_input.getColumnNames();
    std::set_difference(
        all_left_columns.begin(),
        all_left_columns.end(),
        left_join_on_keys.begin(),
        left_join_on_keys.end(),
        std::inserter(non_join_on_columns, non_join_on_columns.begin()));
    set_ndv_for_non_join_on_columns();

    non_join_on_columns.clear();

    /// right table
    auto all_right_columns = right_input.getColumnNames();
    std::set_difference(
        all_right_columns.begin(),
        all_right_columns.end(),
        right_join_on_keys.begin(),
        right_join_on_keys.end(),
        std::inserter(non_join_on_columns, non_join_on_columns.begin()));
    set_ndv_for_non_join_on_columns();
}

void JoinStatsCalculator::Impl::calculateColumnStatsForOuterJoin(Stats & statistics, JoinKind type)
{
    /// For join on columns
    for (size_t i = 0; i < left_join_on_keys.size(); i++)
    {
        const auto & left_key = left_join_on_keys[i];
        const auto & right_key = right_join_on_keys[i];

        const auto left_key_stats = statistics.getColumnStatistics(left_key);
        const auto right_key_stats = statistics.getColumnStatistics(right_key);

        auto left_ndv = left_key_stats->getNdv();
        auto right_ndv = right_key_stats->getNdv();

        switch (type)
        {
            case JoinKind::Left:
                /// left table column ndv not changed
                right_key_stats->setNdv(right_ndv * 0.8); /// TODO add to settings
                /// shrink right column min_value/max_value
                right_key_stats->mergeColumnValueByIntersect(left_key_stats);
                break;
            case JoinKind::Right:
                /// right table column ndv not changed
                left_key_stats->setNdv(left_ndv * 0.8);
                /// shrink right column min_value/max_value
                left_key_stats->mergeColumnValueByIntersect(right_key_stats);
                break;
            case JoinKind::Full:
                /// both left and right table column ndv not changed,
                /// min_value/max_value not changed
                break;
            default:
                /// should never reach here
                break;
        }
    }

    /// For non join on columns
    /// Only calculate ndv, the min_value/max_value remain unchanged.
    Names non_join_on_columns;
    auto set_ndv_for_non_join_on_columns = [&non_join_on_columns, &statistics]()
    {
        for (auto & non_join_on_column : non_join_on_columns)
        {
            auto ndv = statistics.getColumnStatistics(non_join_on_column)->getNdv() * 0.8; /// TODO add to settings
            statistics.getColumnStatistics(non_join_on_column)->setNdv(ndv);
        }
    };

    auto all_right_columns = right_input.getColumnNames();
    auto all_left_columns = left_input.getColumnNames();

    switch (type)
    {
        case JoinKind::Left:
            /// left table column ndv not changed
            std::set_difference(
                all_right_columns.begin(),
                all_right_columns.end(),
                right_join_on_keys.begin(),
                right_join_on_keys.end(),
                std::inserter(non_join_on_columns, non_join_on_columns.begin()));
            set_ndv_for_non_join_on_columns();
            break;
        case JoinKind::Right:
            /// right table column ndv not changed
            std::set_difference(
                all_left_columns.begin(),
                all_left_columns.end(),
                left_join_on_keys.begin(),
                left_join_on_keys.end(),
                std::inserter(non_join_on_columns, non_join_on_columns.begin()));
            set_ndv_for_non_join_on_columns();
            break;
        case JoinKind::Full:
            /// both left and right table column ndv not changed,
            /// min_value/max_value not changed
            break;
        default:
            /// should never reach here
            break;
    }
}

void JoinStatsCalculator::Impl::removeNonOutputColumn(Stats & input)
{
    auto stats_columns = input.getColumnNames();

    if (stats_columns.size() > output_columns.size())
    {
        std::sort(stats_columns.begin(), stats_columns.end());
        std::sort(output_columns.begin(), output_columns.end());

        Names non_output_columns;
        std::set_difference(
            stats_columns.begin(),
            stats_columns.end(),
            output_columns.begin(),
            output_columns.end(),
            std::inserter(non_output_columns, non_output_columns.begin()));

        for (const auto & non_output_column : non_output_columns)
            input.removeColumnStatistics(non_output_column);
    }
}

}
