#pragma once

#include "MatchImpl.h"
#include "FunctionsStringSearch.h"

namespace DB
{

struct NameLike
{
    static constexpr auto name = "like";
};

using LikeImpl = MatchImpl<NameLike, true, false, false>;
using FunctionLike = FunctionsStringSearch<LikeImpl>;

}
