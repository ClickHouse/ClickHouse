#include <Functions/FunctionFactory.h>
#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{
// articat for arrayPackBitsToUInt64
struct ArrayPackBitsToUint64Impl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return true; }


    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeArray>(expression_return);

        //        return std::make_shared<DataTypeUInt8>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        return ColumnArray::create(mapped->convertToFullColumnIfConst(), array.getOffsetsPtr());
    }
};

struct NameArrayPackBitsToUInt64
{
    static constexpr auto name = "arrayPackBitsToUInt64";
};

using FunctionArrayPackBitsToUInt64 = FunctionArrayMapped<ArrayPackBitsToUint64Impl, NameArrayPackBitsToUInt64>;

REGISTER_FUNCTION(ArrayPackBitsToUInt64)
{
    factory.registerFunction<FunctionArrayPackBitsToUInt64>();
}
}
