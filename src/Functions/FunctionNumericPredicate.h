#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context_fwd.h>
#include <base/TypeList.h>
#include <base/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl>
class FunctionNumericPredicate final : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNumericPredicate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!(isNativeNumber(arguments.front()) || WhichDataType(arguments.front()).isBFloat16()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be a number", getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * in = arguments.front().column.get();

        ColumnPtr res;
        TypeListUtils::forEach(
            TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, BFloat16>{},
            [&]<typename T>(TypeList<T>)
            {
                if (!res)
                    res = execute<T>(in, input_rows_count);
            });
        if (!res)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", in->getName(), getName());

        return res;
    }

    template <typename T>
    ColumnPtr execute(const IColumn * in_untyped, size_t input_rows_count) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            auto out = ColumnUInt8::create(input_rows_count);

            const auto & in_data = in->getData();
            auto & out_data = out->getData();

            for (size_t i = 0; i < input_rows_count; ++i)
                out_data[i] = Impl::execute(in_data[i]);

            return out;
        }

        return nullptr;
    }
};

}
