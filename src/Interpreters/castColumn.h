#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Functions/CastOverloadResolver.h>

#include <mutex>
#include <tuple>

namespace DB
{

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

struct InternalCastFunctionCache
{
private:
    /// Maps <cast_type, from_type, to_type> -> cast functions
    /// Doesn't own key, never refer to key after inserted
    std::map<std::tuple<CastType, String, String>, FunctionBasePtr> impl;
    mutable std::mutex mutex;
public:
    template <typename Getter>
    FunctionBasePtr getOrSet(CastType cast_type, const String & from, const String & to, Getter && getter)
    {
        std::lock_guard lock{mutex};
        auto key = std::forward_as_tuple(cast_type, from, to);
        auto it = impl.find(key);
        if (it == impl.end())
            it = impl.emplace(key, getter()).first;
        return it->second;
    }
};

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

/// Accurately cast the non-NULL rows of a materialized column to a non-nullable type. The NULL rows are those
/// of `getSourceNullMap`, and a column without them converts as a whole. Preserve row positions and leave
/// unspecified values at the skipped rows. The caller must retain the source null map to distinguish those
/// values from the converted non-NULL values.
ColumnPtr castColumnAccurateSkipNulls(
    const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

/// The same with `castColumnAccurateOrNull`: return a `Nullable(type)` column whose NULLs are the non-NULL
/// rows that do not convert. The skipped rows are not NULL.
ColumnPtr castColumnAccurateOrNullSkipNulls(
    const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

/// Mark non-NULL values whose `DateTime64` components lose fractional seconds, exceed the destination
/// `DateTime` range, or do not convert back from a floating-point destination to the same timestamp.
/// Follow the cast's structural element mapping, including the active alternative of a `Variant`, and return
/// no column when no check is needed. The mapping is not validated again, so call it after the cast of the
/// same column, which rejects a structure it cannot convert. SQL NULLs are never marked. An all-NULL column
/// and a `Variant` alternative without rows are not visited, so their conversion may have been skipped.
ColumnPtr getDateTime64CastLossMap(const ColumnWithTypeAndName & source, const DataTypePtr & target_type);

}
