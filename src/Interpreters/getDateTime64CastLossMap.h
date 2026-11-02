#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

/// Mark non-NULL values whose `DateTime64` components lose fractional seconds, exceed the destination
/// `DateTime` range, or do not convert back from a floating-point destination to the same timestamp.
/// Follow the cast's structural element mapping, including the active alternative of a `Variant`, and return
/// no column when no check is needed. The mapping is not validated again, so call it after the cast of the
/// same column, which rejects a structure it cannot convert. SQL NULLs are never marked. An all-NULL column
/// and a `Variant` alternative without rows are not visited, so their conversion may have been skipped.
ColumnPtr getDateTime64CastLossMap(const ColumnWithTypeAndName & source, const DataTypePtr & target_type);

}
