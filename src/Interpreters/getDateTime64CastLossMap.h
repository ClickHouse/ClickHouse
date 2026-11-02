#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

struct InternalCastFunctionCache;

/// Mark non-NULL values with a `DateTime64` component that the converted value does not convert back to: the
/// destination drops fractional seconds, the time of day or the date, or saturates a value outside its range.
/// A floating-point destination converts back to the timestamp nearest to it. Follow the cast's structural
/// element mapping, including the active alternative of a `Variant`, and return no column when no check is
/// needed. The mapping is not validated again, so call it after the cast of the same column, which rejects a
/// structure it cannot convert. SQL NULLs are never marked. An all-NULL column and a `Variant` alternative
/// without rows are not visited, so their conversion may have been skipped.
ColumnPtr getDateTime64CastLossMap(
    const ColumnWithTypeAndName & source, const DataTypePtr & target_type, InternalCastFunctionCache * cache = nullptr);

}
