#pragma once
#include <Functions/FunctionsConversion.h>

namespace DB
{

struct CastOverloadName
{
    static constexpr auto cast_name = "CAST";
    static constexpr auto accurate_cast_name = "accurateCast";
    static constexpr auto accurate_cast_or_null_name = "accurateCastOrNull";
};

struct CastInternalOverloadName
{
    static constexpr auto cast_name = "_CAST";
    static constexpr auto accurate_cast_name = "accurate_Cast";
    static constexpr auto accurate_cast_or_null_name = "accurate_CastOrNull";
};

template <CastType cast_type> using CastOverloadResolver = CastOverloadResolverImpl<cast_type, false, CastOverloadName, CastName>;
template <CastType cast_type> using CastInternalOverloadResolver = CastOverloadResolverImpl<cast_type, true, CastInternalOverloadName, CastInternalName>;

}
