#pragma once

#include <base/types.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

class Statistics;

using StatisticsPtr = std::shared_ptr<Statistics>;
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

    void addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats);
    ColumnStatisticsPtr getColumnStatistics(const String & column_name) const;

    const ColumnStatisticsMap & getColumnStatisticsMap() const;

private:
    Float64 output_row_size;
    ColumnStatisticsMap columns_stats_map;
};

}
