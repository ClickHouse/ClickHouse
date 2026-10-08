#pragma once

#include <Core/Field.h>
#include <Core/Types.h>

#include <vector>

namespace DB
{

struct ColumnMinMaxCount
{
    Field min;
    Field max;
    UInt64 non_null_rows = 0;
};

struct MinMaxCountStatistics
{
    UInt64 total_rows = 0;
    std::vector<ColumnMinMaxCount> columns;
};

}
