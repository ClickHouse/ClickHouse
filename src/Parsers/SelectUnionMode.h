#pragma once

#include <unordered_set>
#include <vector>

namespace DB
{
enum class SelectUnionMode
{
    Unspecified,
    ALL,
    DISTINCT,
    EXCEPT,
    INTERSECT
};

using SelectUnionModes = std::vector<SelectUnionMode>;
using SelectUnionModesSet = std::unordered_set<SelectUnionMode>;

}
