#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

ColumnStatistics ColumnStatistics::UNKNOWN = {0.0, 0.0, 0.0, 1.0, nullptr, true, {}};

void testFload64()
{
    Float64 n = 1.0;
    isnan(n);
    isfinite(n);
}

}
