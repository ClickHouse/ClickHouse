#pragma once

#include <base/types.h>
#include <QueryCoordination/NewOptimizer/Statistics/Histogram.h>

namespace DB
{

class ColumnStatistic
{

private:
    Float64 min;
    Float64 max;

    size_t distinct;
    std::optional<Histogram> histogram;
};

}

