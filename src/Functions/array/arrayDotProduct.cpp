#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include "arrayScalarProduct.h"


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
    static DataTypePtr getReturnType(const DataTypePtr & left_type, const DataTypePtr & right_type)
    {
        const auto & common_type = getLeastSupertype(DataTypes{left_type, right_type});
        switch (common_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
           default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    std::string(NameArrayDotProduct::name),
                    common_type->getName());
        }
    }

    template <typename ResultType, typename T, typename U>
    static ResultType apply(
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

/// These functions are used by TupleOrArrayFunction in Function/vectorFunctions.cpp
FunctionPtr createFunctionArrayDotProduct(ContextPtr context_) { return FunctionArrayDotProduct::create(context_); }
}
