#pragma once

#include <cmath>
#include <base/types.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

/// Extract and validate the `p` argument from its column, shared by the same carriers.
/// All of them accept any numeric constant (`UInt*`, `Int*`, `Float*`), so the tuple and
/// array paths of one function cannot drift apart in which `p` types they take. Checking
/// that the column is constant remains up to the caller.
inline Float64 extractLpNormPArgument(const IColumn & column, const String & function_name)
{
    if (!column.isNumeric())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument p of function {} must be a numeric constant",
            function_name);

    Float64 p = column.getFloat64(0);
    checkLpNormPArgument(p, function_name);
    return p;
}

}
