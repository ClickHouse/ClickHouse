#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Functions/CastOverloadResolver.h>
#include <Interpreters/Context_fwd.h>

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

/// Same as above, but threads a real query context so the internal cast honors
/// context-dependent conversion settings (e.g. format settings like
/// json_type_escape_dots_in_keys). Passing a null context keeps the setting-free behavior.
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const ContextPtr & context, InternalCastFunctionCache * cache = nullptr);

}
