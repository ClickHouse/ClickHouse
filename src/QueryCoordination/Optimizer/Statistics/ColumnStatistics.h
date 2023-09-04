#pragma once

#include <math.h>
#include <base/types.h>
#include <QueryCoordination/Optimizer/Statistics/Histogram.h>

namespace DB
{

struct ColumnStatistics;
using ColumnStatisticsMap = std::unordered_map<String, ColumnStatistics>;

struct ColumnStatistics
{
public:
    static ColumnStatistics UNKNOWN;

    Float64 min_value;
    Float64 max_value;

    /// number of distinct value
    Float64 ndv;
    Float64 avg_row_size;

    DataTypePtr data_type;

    bool is_unknown;

    std::optional<Histogram> histogram;


};

}

