#pragma once

#include <base/types.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

class Statistics;
using StatisticsList = std::vector<Statistics>;

class Statistics
{
public:
    void setOutputRowSize(Float64 row_size)
    {
        output_row_size = row_size;
    }

    Float64 getOutputRowSize() const
    {
        return output_row_size;
    }

    void addColumnStatistics(const String & column_name, ColumnStatistics column_stats);
    ColumnStatistics & getColumnStatistics(const String & column_name);

private:
    Float64 output_row_size;
    ColumnStatisticsMap columns_stats_map;
};

}
