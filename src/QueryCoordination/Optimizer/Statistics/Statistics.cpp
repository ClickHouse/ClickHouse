#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

void Statistics::addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats)
{
    columns_stats_map.insert({column_name, column_stats});
}

ColumnStatisticsPtr Statistics::getColumnStatistics(const String & column_name) const
{
    if (columns_stats_map.contains(column_name))
        return columns_stats_map.at(column_name);
    return ColumnStatistics::unknown();
}

const ColumnStatisticsMap & Statistics::getColumnStatisticsMap() const
{
    return columns_stats_map;
}

}
