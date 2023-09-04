#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

void Statistics::addColumnStatistics(const String & column_name, ColumnStatistics column_stats)
{
    columns_stats_map.insert({column_name, column_stats});
}
ColumnStatistics & Statistics::getColumnStatistics(const String & column_name)
{
    return columns_stats_map.at(column_name);
}

}
