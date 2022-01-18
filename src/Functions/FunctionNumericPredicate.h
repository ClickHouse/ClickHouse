#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context_fwd.h>
#include <common/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl>
class FunctionNumericPredicate : public IFunction
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNativeNumber(arguments.front()))
            throw Exception{"Argument for function " + getName() + " must be number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto * in = arguments.front().column.get();

        ColumnPtr res;
        if (!((res = execute<UInt8>(in))
            || (res = execute<UInt16>(in))
            || (res = execute<UInt32>(in))
            || (res = execute<UInt64>(in))
            || (res = execute<Int8>(in))
            || (res = execute<Int16>(in))
            || (res = execute<Int32>(in))
            || (res = execute<Int64>(in))
            || (res = execute<Float32>(in))
            || (res = execute<Float64>(in))))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        return res;
    }

    template <typename T>
    ColumnPtr execute(const IColumn * in_untyped) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto size = in->size();

            auto out = ColumnUInt8::create(size);

            const auto & in_data = in->getData();
            auto & out_data = out->getData();

            for (const auto i : collections::range(0, size))
                out_data[i] = Impl::execute(in_data[i]);

            return out;
        }

        return nullptr;
    }
};

}
