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
    static Statistics unknown(const Names & column_names);
    Statistics clone() const;

    void setOutputRowSize(Float64 row_size);
    Float64 getOutputRowSize() const;

    void addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats);
    void removeColumnStatistics(const String & column_name);

    ColumnStatisticsPtr getColumnStatistics(const String & column_name) const;
    bool hasUnknownColumn() const;

    const ColumnStatisticsMap & getColumnStatisticsMap() const;
    void adjustStatistics();

    void mergeColumnByUnion(const String & column_name, ColumnStatisticsPtr other);

private:
    Float64 output_row_size;
    ColumnStatisticsMap columns_stats_map;
};

}
