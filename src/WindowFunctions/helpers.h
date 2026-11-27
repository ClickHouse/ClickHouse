#pragma once

#include <base/types.h>

namespace DB
{

class WindowTransform;
struct RowNumber;

namespace WindowRowAccess
{

Float64 getArgumentFloat64(const WindowTransform * transform, size_t function_index, size_t argument_index, RowNumber row);
void insertResultFloat64(const WindowTransform * transform, size_t function_index, Float64 value);
bool isPartitionFirstRow(const WindowTransform * transform);
bool isPartitionLastRow(const WindowTransform * transform);

}

}
