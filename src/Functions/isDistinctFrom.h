#pragma once
#include <Core/AccurateComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>
namespace DB
{

struct NameFunctionIsDistinctFrom { static constexpr auto name = "isDistinctFrom"; };

using NullSafeNotEqualImpl = DB::FunctionsNullSafeCmp<NameFunctionIsDistinctFrom,
                                                      NullSafeCmpMode::NullSafeNotEqual,
                                                      NotEqualsOp,
                                                      NameNotEquals>;
using FunctionIsDistinctFrom = NullSafeNotEqualImpl;

}
