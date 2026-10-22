#pragma once

#include <string>
#include <unordered_set>
#include <vector>

namespace DB
{
enum class SelectUnionMode : uint8_t
{
    UNION_DEFAULT = 0,
    UNION_ALL,
    UNION_DISTINCT,
    EXCEPT_DEFAULT,
    EXCEPT_ALL,
    EXCEPT_DISTINCT,
    INTERSECT_DEFAULT,
    INTERSECT_ALL,
    INTERSECT_DISTINCT
};

const char * toString(SelectUnionMode mode);
SelectUnionMode parseSelectUnionMode(const std::string & str);

using SelectUnionModes = std::vector<SelectUnionMode>;
using SelectUnionModesSet = std::unordered_set<SelectUnionMode>;

}
