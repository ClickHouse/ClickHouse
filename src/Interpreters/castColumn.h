#pragma once

#include <tuple>
#include <Core/ColumnWithTypeAndName.h>
#include <Functions/FunctionsConversion.h>

namespace DB
{

struct InternalCastFunctionCache
{
private:
    /// Maps <cast_type, from_type, to_type> -> cast functions
    /// Doesn't own key, never refer to key after inserted
    std::map<std::tuple<CastType, std::string_view, std::string_view>, FunctionBasePtr> impl;
    mutable std::mutex mutex;
public:
    template<typename Getter>
    FunctionBasePtr getOrSet(CastType cast_type, const String & from, const String & to, Getter && getter)
    {
        std::lock_guard lock{mutex};
        auto key = std::make_tuple(cast_type, std::string_view{from}, std::string_view{to});
        auto it = impl.find(key);
        if (it == impl.end())
            it = impl.emplace(key, getter()).first;
        return it->second;
    }
};

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

}
