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
    std::map<std::tuple<CastType, String, String, bool>, FunctionBasePtr> impl;
    mutable std::mutex mutex;
public:
    template <typename Getter>
    FunctionBasePtr getOrSet(CastType cast_type, const String & from, const String & to, bool fixed_string_to_string_strip_trailing_zeros, Getter && getter)
    {
        std::lock_guard lock{mutex};
        auto key = std::forward_as_tuple(cast_type, from, to, fixed_string_to_string_strip_trailing_zeros);
        auto it = impl.find(key);
        if (it == impl.end())
            it = impl.emplace(key, getter()).first;
        return it->second;
    }
};

/// The cast runs without a query context. `fixed_string_to_string_strip_trailing_zeros` stands for the setting
/// `cast_fixed_string_to_string_strip_trailing_zeros`, for the callers that have to follow it.
ColumnPtr castColumn(
    const ColumnWithTypeAndName & arg,
    const DataTypePtr & type,
    InternalCastFunctionCache * cache = nullptr,
    bool fixed_string_to_string_strip_trailing_zeros = false);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

}
