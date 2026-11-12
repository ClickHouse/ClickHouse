#pragma once

#include <Core/AccurateComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>


namespace DB
{

struct NameFunctionIsNotDistinctFrom { static constexpr auto name = "isNotDistinctFrom"; };
struct NameFunctionExactNumericIsNotDistinctFrom { static constexpr auto name = "__exactNumericIsNotDistinctFrom"; };

using NullSafeEqualImpl = FunctionsNullSafeCmp<NameFunctionIsNotDistinctFrom,
                                               NullSafeCmpMode::NullSafeEqual,
                                               EqualsOp,
                                               NameEquals>;
using FunctionIsNotDistinctFrom = NullSafeEqualImpl;
using FunctionExactNumericIsNotDistinctFrom = FunctionsNullSafeCmp<NameFunctionExactNumericIsNotDistinctFrom,
    NullSafeCmpMode::NullSafeEqual, EqualsOp, NameEquals, true>;

}
