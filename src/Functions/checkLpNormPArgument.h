#pragma once

#include <cmath>
#include <base/types.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>

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

/// Validate the type of the `p` argument during return-type inference, so analysis-only paths
/// (e.g. `toTypeName`) reject a non-numeric `p` instead of advertising a return type, consistently
/// with the tuple carriers, which type-check `p` while building `pow` in their `getReturnTypeImpl`.
/// The accepted set matches `extractLpNormPArgument` below: every numeric type whose column is
/// `isNumeric` (native and wide integers, floats), but not `Decimal`.
inline void checkLpNormPArgumentType(const IDataType & p_type, const String & function_name)
{
    WhichDataType which(p_type);
    if (!which.isInteger() && !which.isFloat())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument p of function {} must be a numeric constant",
            function_name);
}

/// Validate the `p` argument during return-type inference. On top of the type, validate the value
/// itself whenever `p` is already known as a constant at analysis time, so that analysis-only paths
/// (e.g. `toTypeName`) reject an out-of-range `p` such as `0.5`, `nan` or `inf` instead of
/// advertising a return type for a call that execution rejects with `ARGUMENT_OUT_OF_BOUND`.
inline void checkLpNormPArgumentForAnalysis(const ColumnWithTypeAndName & p_argument, const String & function_name)
{
    checkLpNormPArgumentType(*p_argument.type, function_name);

    if (p_argument.column && isColumnConst(*p_argument.column))
        checkLpNormPArgument(p_argument.column->getFloat64(0), function_name);
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
