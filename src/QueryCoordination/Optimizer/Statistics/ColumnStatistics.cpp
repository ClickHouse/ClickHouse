#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

ColumnStatisticsPtr ColumnStatistics::unknown()
{
    return std::make_shared<ColumnStatistics>();
}

ColumnStatisticsPtr ColumnStatistics::create(Float64 value)
{
    return std::make_shared<ColumnStatistics>(value);
}

ColumnStatisticsPtr ColumnStatistics::clone()
{
    return std::make_shared<ColumnStatistics>(min_value, max_value, ndv, avg_row_size, data_type, is_unknown, histogram);
}

Float64 ColumnStatistics::calculateSelectivity(OP_TYPE, Float64 /*value*/)
{
    return 1.0; /// TODO
}

void ColumnStatistics::updateValues(OP_TYPE, Float64 /*value*/)
{
    /// TODO
}

void testFload64()
{
    Float64 n = 1.0;
    isnan(n);
    isfinite(n);
}

}
