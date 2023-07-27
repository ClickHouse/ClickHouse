#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Core/Types_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/castTypeToEither.h>
#include <Functions/array/arrayScalarProduct.h>
#include <base/types.h>
#include <Functions/FunctionBinaryArithmetic.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct NameArrayDotProduct
{
    static constexpr auto name = "arrayDotProduct";
};

class ArrayDotProductImpl
{
public:
    static DataTypePtr getReturnType(const DataTypePtr & left, const DataTypePtr & right)
    {
        using Types = TypeList<DataTypeFloat32, DataTypeFloat64,
                               DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                               DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;

        DataTypePtr result_type;
        bool valid = castTypeToEither(Types{}, left.get(), [&](const auto & left_)
        {
            return castTypeToEither(Types{}, right.get(), [&](const auto & right_)
            {
                using LeftDataType = typename std::decay_t<decltype(left_)>::FieldType;
                using RightDataType = typename std::decay_t<decltype(right_)>::FieldType;
                using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<LeftDataType, RightDataType>::Type;
                if (std::is_same_v<LeftDataType, Float32> && std::is_same_v<RightDataType, Float32>)
                    result_type = std::make_shared<DataTypeFloat32>();
                else
                    result_type = std::make_shared<DataTypeFromFieldType<ResultType>>();
                return true;
            });
        });

        if (!valid)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments of function {} "
                "only support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                std::string(NameArrayDotProduct::name));
        return result_type;
    }

    template <typename ResultType, typename T, typename U>
    static inline NO_SANITIZE_UNDEFINED ResultType apply(
        const T * left,
        const U * right,
        size_t size)
    {
        ResultType result = 0;
        for (size_t i = 0; i < size; ++i)
            result += static_cast<ResultType>(left[i]) * static_cast<ResultType>(right[i]);
        return result;
    }
};

using FunctionArrayDotProduct = FunctionArrayScalarProduct<ArrayDotProductImpl, NameArrayDotProduct>;

REGISTER_FUNCTION(ArrayDotProduct)
{
    factory.registerFunction<FunctionArrayDotProduct>();
}

// These functions are used by TupleOrArrayFunction in Function/vectorFunctions.cpp
FunctionPtr createFunctionArrayDotProduct(ContextPtr context_) { return FunctionArrayDotProduct::create(context_); }
}
