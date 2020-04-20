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
    struct ZCurveOpImpl
    {
        using ResultType = UInt64;
        /// Decode given z value range using the decode function. Always decodes into one range,
        /// since the transformation is monotonous.
        static std::vector<std::pair<ResultType, ResultType>> decodeRange(
                ResultType left,
                ResultType right,
                const DataTypePtr & type,
                size_t significant_digits);
        /// Encodes different types monotonously into unsigned 64 bit integer representations.
        static void encode(ResultType& num, const DataTypePtr & type);
        /// Decode the initial number given type and unsigned 64-bit integer representation.
        static ResultType decode(ResultType num, const DataTypePtr & type, bool is_left, size_t significant_digits);
    };

    void registerFunctionZCurve(FunctionFactory & factory);
}
