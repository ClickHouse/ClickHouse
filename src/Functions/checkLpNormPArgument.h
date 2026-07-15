#pragma once

#include <cmath>
#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

/// Validate the `p` argument shared by the tuple and array carriers of `LpNorm`, `LpNormalize`
/// and `LpDistance`. A naive `p < 1 || p >= HUGE_VAL` check lets `NaN` through, because both
/// comparisons are false for `NaN`, so reject non-finite `p` explicitly to keep it within the
/// documented `[1, inf)` range.
inline void checkLpNormPArgument(Float64 p, const String & function_name)
{
    if (!std::isfinite(p) || p < 1)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Second argument for function {} must be a finite number not less than one",
            function_name);
}

}
