#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Core/TypeId.h>


namespace DB
{

class Context;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


template <typename Method, typename Name>
class FunctionArrayScalarProduct : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayScalarProduct>(); }

private:

    template <typename ResultType, typename T>
    ColumnPtr executeNumber(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr res;
        if (   (res = executeNumberNumber<ResultType, T, UInt8>(arguments))
            || (res = executeNumberNumber<ResultType, T, UInt16>(arguments))
            || (res = executeNumberNumber<ResultType, T, UInt32>(arguments))
            || (res = executeNumberNumber<ResultType, T, UInt64>(arguments))
            || (res = executeNumberNumber<ResultType, T, Int8>(arguments))
            || (res = executeNumberNumber<ResultType, T, Int16>(arguments))
            || (res = executeNumberNumber<ResultType, T, Int32>(arguments))
            || (res = executeNumberNumber<ResultType, T, Int64>(arguments))
            || (res = executeNumberNumber<ResultType, T, Float32>(arguments))
            || (res = executeNumberNumber<ResultType, T, Float64>(arguments)))
            return res;

       return nullptr;
    }


    template <typename ResultType, typename T, typename U>
    ColumnPtr executeNumberNumber(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr col1 = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col2 = arguments[1].column->convertToFullColumnIfConst();
        if (!col1 || !col2)
            return nullptr;

        const ColumnArray * col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        const ColumnArray * col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (!col_array1 || !col_array2)
            return nullptr;

        if (!col_array1->hasEqualOffsets(*col_array2))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Array arguments for function {} must have equal sizes", getName());

        const ColumnVector<T> * col_nested1 = checkAndGetColumn<ColumnVector<T>>(col_array1->getData());
        const ColumnVector<U> * col_nested2 = checkAndGetColumn<ColumnVector<U>>(col_array2->getData());
        if (!col_nested1 || !col_nested2)
            return nullptr;

        auto col_res = ColumnVector<ResultType>::create();

        vector(
            col_nested1->getData(),
            col_nested2->getData(),
            col_array1->getOffsets(),
            col_res->getData());

        return col_res;
    }

    template <typename ResultType, typename T, typename U>
    static NO_INLINE void vector(
        const PaddedPODArray<T> & data1,
        const PaddedPODArray<U> & data2,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            result[i] = Method::template apply<ResultType, T, U>(&data1[current_offset], &data2[current_offset], array_size);
            current_offset = offsets[i];
        }
    }

public:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        // Basic type check
        std::vector<DataTypePtr> nested_types(2, nullptr);
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "All arguments for function {} must be an array.", getName());

            const auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type) && !isEnum(nested_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} cannot process values of type {}",
                                getName(), nested_type->getName());
            nested_types[i] = nested_type;
        }

        // Detail type check in Method, then return ReturnType
        return Method::getReturnType(nested_types[0], nested_types[1]);
    }

    template <typename ResultType>
    ColumnPtr executeWithResultType(const ColumnsWithTypeAndName & arguments) const
    {
        ColumnPtr res;
        if (!((res = executeNumber<ResultType, UInt8>(arguments))
            || (res = executeNumber<ResultType, UInt16>(arguments))
            || (res = executeNumber<ResultType, UInt32>(arguments))
            || (res = executeNumber<ResultType, UInt64>(arguments))
            || (res = executeNumber<ResultType, Int8>(arguments))
            || (res = executeNumber<ResultType, Int16>(arguments))
            || (res = executeNumber<ResultType, Int32>(arguments))
            || (res = executeNumber<ResultType, Int64>(arguments))
            || (res = executeNumber<ResultType, Float32>(arguments))
            || (res = executeNumber<ResultType, Float64>(arguments))))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        return res;
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
};

}

