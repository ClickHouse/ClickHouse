#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

void Statistics::addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats)
{
    columns_stats_map.insert({column_name, column_stats});
}
void Statistics::removeColumnStatistics(const String & column_name)
{
    columns_stats_map.erase(column_name);
}

ColumnStatisticsPtr Statistics::getColumnStatistics(const String & column_name) const
{
    if (columns_stats_map.contains(column_name))
        return columns_stats_map.at(column_name);
    return {};
}

const ColumnStatisticsMap & Statistics::getColumnStatisticsMap() const
{
    return columns_stats_map;
}

bool Statistics::hasUnknownColumn() const
{
    for (auto column_stats : columns_stats_map)
    {
        if (column_stats.second->isUnKnown())
            return true;
    }
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


}
