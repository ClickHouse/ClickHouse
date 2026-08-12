#pragma once

#include <unordered_set>
#include <vector>


namespace DB
{

using ColumnNumbers = std::vector<size_t>;
using ColumnNumbersSet = std::unordered_set<size_t>;
using ColumnNumbersList = std::vector<ColumnNumbers>;
using ColumnNumbersSetList = std::vector<ColumnNumbersSet>;

}
