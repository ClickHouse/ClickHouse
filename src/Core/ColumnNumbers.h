#pragma once

#include <unordered_set>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

using ColumnNumbers = std::vector<size_t>;
using ColumnNumbersSet = std::unordered_set<size_t>;
using ColumnNumbersList = VectorWithMemoryTracking<ColumnNumbers>;
using ColumnNumbersSetList = VectorWithMemoryTracking<ColumnNumbersSet>;

}
