#pragma once
#include <Core/AccurateComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>
namespace DB
{

struct NameFunctionIsDistinctFrom { static constexpr auto name = "isDistinctFrom"; };
struct NameFunctionExactNumericIsDistinctFrom { static constexpr auto name = "__exactNumericIsDistinctFrom"; };

using NullSafeNotEqualImpl = DB::FunctionsNullSafeCmp<NameFunctionIsDistinctFrom,
                                                      NullSafeCmpMode::NullSafeNotEqual,
                                                      NotEqualsOp,
                                                      NameNotEquals>;
using FunctionIsDistinctFrom = NullSafeNotEqualImpl;
using FunctionExactNumericIsDistinctFrom = FunctionsNullSafeCmp<NameFunctionExactNumericIsDistinctFrom,
    NullSafeCmpMode::NullSafeNotEqual, NotEqualsOp, NameNotEquals, true>;

}
