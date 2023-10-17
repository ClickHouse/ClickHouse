#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Functions/FunctionsConversion.h>

namespace DB
{

struct InternalCastFunctionCache
{
    /// Maps <cast_type, from_type, to_type> -> cast functions
    std::map<std::tuple<CastType, std::string_view, std::string_view>, FunctionBasePtr> impl;
    mutable std::mutex mutex;
    using Getter = std::function<FunctionBasePtr()>;
    FunctionBasePtr getOrSet(CastType cast_type, const String & from, const String & to, Getter && getter);
};

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, InternalCastFunctionCache * cache = nullptr);

}
