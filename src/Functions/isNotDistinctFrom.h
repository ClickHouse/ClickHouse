#pragma once
#include <Core/AccurateComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>
namespace DB
{

struct NameFunctionIsNotDistinctFrom { static constexpr auto name = "isNotDistinctFrom"; };

using NullSafeEqualImpl = FunctionsNullSafeCmp<NameFunctionIsNotDistinctFrom,
                                               NullSafeCmpMode::NullSafeEqual,
                                               EqualsOp,
                                               NameEquals>;
using FunctionIsNotDistinctFrom = NullSafeEqualImpl;

}
