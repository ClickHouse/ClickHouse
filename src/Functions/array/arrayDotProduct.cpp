#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

template <typename Impl, typename Name>
class FunctionArrayScalarProduct : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayScalarProduct>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        std::array<DataTypePtr, 2> nested_types;
        for (size_t i = 0; i < 2; ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Arguments for function {} must be of type Array", getName());

            const auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} cannot process values of type {}", getName(), nested_type->getName());

            nested_types[i] = nested_type;
        }

        return Impl::getReturnType(nested_types[0], nested_types[1]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /* input_rows_count */) const override
    {
        switch (result_type->getTypeId())
        {
        #define SUPPORTED_TYPE(type) \
            case TypeIndex::type: \
                return executeWithResultType<type>(arguments); \
                break;

            SUPPORTED_TYPE(UInt8)
            SUPPORTED_TYPE(UInt16)
            SUPPORTED_TYPE(UInt32)
            SUPPORTED_TYPE(UInt64)
            SUPPORTED_TYPE(Int8)
            SUPPORTED_TYPE(Int16)
            SUPPORTED_TYPE(Int32)
            SUPPORTED_TYPE(Int64)
            SUPPORTED_TYPE(Float32)
            SUPPORTED_TYPE(Float64)
        #undef SUPPORTED_TYPE

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }

private:
    template <typename ResultType>
    ColumnPtr executeWithResultType(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr res;
        if (!((res = executeWithResultTypeAndLeft<ResultType, UInt8>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, UInt16>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, UInt32>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, UInt64>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Int8>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Int16>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Int32>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Int64>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Float32>(arguments))
            || (res = executeWithResultTypeAndLeft<ResultType, Float64>(arguments))))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        return res;
    }

    template <typename ResultType, typename LeftType>
    ColumnPtr executeWithResultTypeAndLeft(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr res;
        if (   (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, UInt8>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, UInt16>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, UInt32>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, UInt64>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Int8>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Int16>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Int32>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Int64>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Float32>(arguments))
            || (res = executeWithResultTypeAndLeftAndRight<ResultType, LeftType, Float64>(arguments)))
            return res;

       return nullptr;
    }

    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithResultTypeAndLeftAndRight(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr col_left = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col_right = arguments[1].column->convertToFullColumnIfConst();
        if (!col_left || !col_right)
            return nullptr;

        const ColumnArray * col_arr_left = checkAndGetColumn<ColumnArray>(col_left.get());
        const ColumnArray * cokl_arr_right = checkAndGetColumn<ColumnArray>(col_right.get());
        if (!col_arr_left || !cokl_arr_right)
            return nullptr;

        const ColumnVector<LeftType> * col_arr_nested_left = checkAndGetColumn<ColumnVector<LeftType>>(col_arr_left->getData());
        const ColumnVector<RightType> * col_arr_nested_right = checkAndGetColumn<ColumnVector<RightType>>(cokl_arr_right->getData());
        if (!col_arr_nested_left || !col_arr_nested_right)
            return nullptr;

        if (!col_arr_left->hasEqualOffsets(*cokl_arr_right))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Array arguments for function {} must have equal sizes", getName());

        auto col_res = ColumnVector<ResultType>::create();

        vector(
            col_arr_nested_left->getData(),
            col_arr_nested_right->getData(),
            col_arr_left->getOffsets(),
            col_res->getData());

        return col_res;
    }

    template <typename ResultType, typename LeftType, typename RightType>
    static NO_INLINE void vector(
        const PaddedPODArray<LeftType> & left,
        const PaddedPODArray<RightType> & right,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            result[i] = Impl::template apply<ResultType, LeftType, RightType>(&left[current_offset], &right[current_offset], array_size);
            current_offset = offsets[i];
        }
    }

};

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
        Types types;

        DataTypePtr result_type;
        bool valid = castTypeToEither(types, left.get(), [&](const auto & left_)
        {
            return castTypeToEither(types, right.get(), [&](const auto & right_)
            {
                using LeftType = typename std::decay_t<decltype(left_)>::FieldType;
                using RightType = typename std::decay_t<decltype(right_)>::FieldType;
                using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<LeftType, RightType>::Type;

                if constexpr (std::is_same_v<LeftType, Float32> && std::is_same_v<RightType, Float32>)
                    result_type = std::make_shared<DataTypeFloat32>();
                else
                    result_type = std::make_shared<DataTypeFromFieldType<ResultType>>();
                return true;
            });
        });

        if (!valid)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments of function {} only support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                NameArrayDotProduct::name);
        return result_type;
    }

    template <typename ResultType, typename LeftType, typename RightType>
    static NO_SANITIZE_UNDEFINED ResultType apply(
        const LeftType * left,
        const RightType * right,
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
