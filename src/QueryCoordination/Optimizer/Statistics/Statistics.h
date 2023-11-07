#pragma once

#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>
#include <base/types.h>

namespace DB
{

class Statistics;

using StatisticsPtr = std::shared_ptr<Statistics>;
using StatisticsList = std::vector<Statistics>;

class Statistics
{
public:
    static Statistics unknown(const Names & column_names);

    Statistics() = default;
    Statistics(Float64 row_count, ColumnStatisticsMap column_stats) : output_row_size(row_count), columns_stats_map(column_stats) { }

    Statistics clone() const;
    StatisticsPtr clonePtr() const;

    void setOutputRowSize(Float64 row_size);
    Float64 getOutputRowSize() const;

    void addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats);
    void removeColumnStatistics(const String & column_name);

    bool containsColumnStatistics(const String & column_name) const;
    ColumnStatisticsPtr getColumnStatistics(const String & column_name) const;

    void addAllColumnsFrom(const Statistics & other);

    size_t getColumnStatisticsSize() const;
    Names getColumnNames() const;

    bool hasUnknownColumn() const;
    bool hasUnknownColumn(const Names & columns) const;

    void adjustStatistics();
    void mergeColumnValueByUnion(const String & column_name, ColumnStatisticsPtr other);

    void reset();

    /// Used to calculate cost
    Float64 getDataSize() const;

private:
    Float64 output_row_size;
    ColumnStatisticsMap columns_stats_map;
};

}
