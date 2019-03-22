#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <Functions/zCurveBase.h>
#include <Common/RadixSort.h>

namespace DB
{
    struct NameZCurve;
    struct ZCurveOpImpl {
        using ResultType = UInt64;
        static std::vector<std::pair<ResultType, ResultType>> decodeRange(
                ResultType left,
                ResultType right,
                const DataTypePtr & type,
                size_t significant_digits);
        static void encode(ResultType& num, const DataTypePtr & type);
        static ResultType decode(ResultType num, const DataTypePtr & type, bool is_left, size_t significant_digits);
    };

    void registerFunctionZCurve(FunctionFactory & factory);
}
