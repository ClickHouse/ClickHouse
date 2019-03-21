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

    struct NameZCurve { static constexpr auto name = "zCurve"; };

    struct ZCurveOpImpl {
        using ResultType = UInt64;
        static void encode(ResultType& num, const DataTypePtr & type)
        {
            auto type_id = type->getTypeId();
            if (type_id == TypeIndex::Int8)
            {
                num ^= static_cast<UInt8>(std::numeric_limits<Int8>::min());
            }
            if (type_id == TypeIndex::Int16)
            {
                num ^= static_cast<UInt16>(std::numeric_limits<Int16>::min());
            }
            if (type_id == TypeIndex::Int32)
            {
                num ^= static_cast<UInt32>(std::numeric_limits<Int32>::min());
            }
            if (type_id == TypeIndex::Int64)
            {
                num ^= static_cast<UInt64>(std::numeric_limits<Int64>::min());
            }
            if (type_id == TypeIndex::Float32)
            {
                num = RadixSortFloatTransform<UInt32>::forward(static_cast<UInt32>(num));
            }
            if (type_id == TypeIndex::Float64)
            {
                num = RadixSortFloatTransform<UInt64>::forward(num);
            }
            num <<= ((sizeof(ResultType) - type->getSizeOfValueInMemory()) << 3);
        }
        static void decode(ResultType& num, const DataTypePtr & type)
        {
            num >>= ((sizeof(ResultType) - type->getSizeOfValueInMemory()) << 3);
            auto type_id = type->getTypeId();
            if (type_id == TypeIndex::Int8)
            {
                num ^= static_cast<UInt8>(std::numeric_limits<Int8>::min());
            }
            if (type_id == TypeIndex::Int16)
            {
                num ^= static_cast<UInt16>(std::numeric_limits<Int16>::min());
            }
            if (type_id == TypeIndex::Int32)
            {
                num ^= static_cast<UInt32>(std::numeric_limits<Int32>::min());
            }
            if (type_id == TypeIndex::Int64)
            {
                num ^= static_cast<UInt64>(std::numeric_limits<Int64>::min());
            }
            if (type_id == TypeIndex::Float32)
            {
                const int EXP = 9, SZ = (sizeof(UInt32) << 3);
                UInt32 INF = ((static_cast<UInt32>(1) << EXP) - 1) << (SZ - EXP);
                UInt32 NEG_INF = (static_cast<UInt32>(1) << (SZ - EXP)) - 1;
                if (num > INF) {
                    num = INF;
                } else if (num < NEG_INF) {
                    num = NEG_INF;
                }
                num = RadixSortFloatTransform<UInt32>::backward(static_cast<UInt32>(num));
            }
            if (type_id == TypeIndex::Float64)
            {
                const int EXP = 11, SZ = (sizeof(UInt64) << 3);
                UInt64 INF = ((static_cast<UInt64>(1) << EXP) - 1) << (SZ - EXP);
                UInt64 NEG_INF = (static_cast<UInt64>(1) << (SZ - EXP)) - 1;
                if (num > INF) {
                    num = INF;
                } else if (num < NEG_INF) {
                    num = NEG_INF;
                }
                num = RadixSortFloatTransform<UInt64>::backward(num);
            }
        }
    };

    using FunctionZCurve = FunctionZCurveBase<ZCurveOpImpl, NameZCurve>;

    void registerFunctionZCurve(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionZCurve>();
    }
}
