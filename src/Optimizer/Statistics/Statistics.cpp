#include <Optimizer/Statistics/Statistics.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

Statistics Statistics::unknown(const Names & column_names)
{
    Statistics statistics;
    statistics.setOutputRowSize(1.0);

    for (const auto & column_name : column_names)
        statistics.addColumnStatistics(column_name, ColumnStatistics::unknown());
    return statistics;
}

Statistics Statistics::clone() const
{
    Statistics statistics;
    statistics.setOutputRowSize(output_row_size);

    for (const auto & column_stats : columns_stats_map)
        statistics.addColumnStatistics(column_stats.first, column_stats.second->clone());

    return statistics;
}

StatisticsPtr Statistics::clonePtr() const
{
    auto statistics = std::make_shared<Statistics>();
    statistics->setOutputRowSize(output_row_size);

    for (const auto & column_stats : columns_stats_map)
        statistics->addColumnStatistics(column_stats.first, column_stats.second->clone());

    return statistics;
}


void Statistics::setOutputRowSize(Float64 row_size)
{
    output_row_size = std::max(1.0, row_size);
}

Float64 Statistics::getOutputRowSize() const
{
    return output_row_size;
}

void Statistics::addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats)
{
    if (columns_stats_map.contains(column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Already exists statistics for column {}", column_name);
    columns_stats_map.insert({column_name, column_stats});
}

void Statistics::removeColumnStatistics(const String & column_name)
{
    if (!columns_stats_map.contains(column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No statistics for column {}", column_name);
    columns_stats_map.erase(column_name);
}

bool Statistics::containsColumnStatistics(const String & column_name) const
{
    return columns_stats_map.contains(column_name);
}

ColumnStatisticsPtr Statistics::getColumnStatistics(const String & column_name) const
{
    if (!columns_stats_map.contains(column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No statistics for column {}", column_name);
    return columns_stats_map.at(column_name);
}

size_t Statistics::getColumnStatisticsSize() const
{
    return columns_stats_map.size();
}

Names Statistics::getColumnNames() const
{
    Names columns;
    for (const auto & entry : columns_stats_map)
        columns.push_back(entry.first);
    return columns;
}

bool Statistics::hasUnknownColumn() const
{
    for (auto column_stats : columns_stats_map)
        if (column_stats.second->isUnKnown())
            return true;
    return false;
}

bool Statistics::hasUnknownColumn(const Names & columns) const
{
    for (auto column : columns)
        if (getColumnStatistics(column)->isUnKnown())
            return true;
    return false;
}

void Statistics::adjustStatistics()
{
    for (auto column_stats_entry : columns_stats_map)
    {
        auto & column_stats = column_stats_entry.second;
        if (!column_stats->isUnKnown() && column_stats->getNdv() > output_row_size)
            column_stats->setNdv(output_row_size);
    }
}

void Statistics::mergeColumnValueByUnion(const String & column_name, ColumnStatisticsPtr other)
{
    if (!other)
        return;
    auto my = getColumnStatistics(column_name);
    my->mergeColumnValueByUnion(other);
}


void Statistics::addAllColumnsFrom(const Statistics & other)
{
    for (auto & entry : other.columns_stats_map)
    {
        if (columns_stats_map.contains(entry.first))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Already exists statistics for column {}", entry.first);
        columns_stats_map.insert(entry); /// TODO clone
    }
}

void Statistics::reset()
{
    output_row_size = 1.0;
    columns_stats_map.clear();
}

Float64 Statistics::getDataSize() const
{
    Float64 total_row_size{};
    for (auto & [_, column_stats] : columns_stats_map)
        total_row_size += column_stats->isUnKnown() ? 8 : column_stats->getAvgRowSize();

    if (total_row_size <= 0)
        total_row_size = 8;

    return total_row_size * output_row_size;
}

}
