#pragma once

#include <string>
#include <unordered_set>
#include <vector>
#include <cstdint>

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

enum class SetOperationColumnMatchMode : uint8_t
{
    Position = 0,
    Name,
};

struct SetOperationDescriptor
{
    SelectUnionMode mode = SelectUnionMode::UNION_DEFAULT;
    SetOperationColumnMatchMode column_match_mode = SetOperationColumnMatchMode::Position;
};

const char * toString(SelectUnionMode mode);
SelectUnionMode parseSelectUnionMode(const std::string & str);
const char * toString(SetOperationColumnMatchMode mode);
SetOperationColumnMatchMode parseSetOperationColumnMatchMode(const std::string & str);

using SelectUnionModes = std::vector<SelectUnionMode>;
using SelectUnionModesSet = std::unordered_set<SelectUnionMode>;
using SetOperationColumnMatchModes = std::vector<SetOperationColumnMatchMode>;
using SetOperationDescriptors = std::vector<SetOperationDescriptor>;

}
