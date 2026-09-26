#pragma once

#include <Core/ColumnWithTypeAndName.h>

#include <vector>

namespace DB
{

using ColumnsWithTypeAndName = VectorWithMemoryTracking<ColumnWithTypeAndName>;

}
