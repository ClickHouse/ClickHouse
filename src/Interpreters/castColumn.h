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

/// `context`, when not null, is forwarded to the internal cast so it honors query settings such as
/// `cast_float_to_decimal_uses_rounding`. Defaults to null, which preserves the previous behavior
/// (the cast uses default settings).
ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr, ContextPtr context = nullptr);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr, ContextPtr context = nullptr);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr, ContextPtr context = nullptr);

}
