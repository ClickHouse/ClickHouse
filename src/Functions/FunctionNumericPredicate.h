#pragma once
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <ext/range.h>


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
    static FunctionPtr create(const Context &)
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

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const auto in = columns[arguments.front()].column.get();

        if (   !execute<UInt8>(columns, in, result)
            && !execute<UInt16>(columns, in, result)
            && !execute<UInt32>(columns, in, result)
            && !execute<UInt64>(columns, in, result)
            && !execute<Int8>(columns, in, result)
            && !execute<Int16>(columns, in, result)
            && !execute<Int32>(columns, in, result)
            && !execute<Int64>(columns, in, result)
            && !execute<Float32>(columns, in, result)
            && !execute<Float64>(columns, in, result))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename T>
    bool execute(ColumnsWithTypeAndName & columns, const IColumn * in_untyped, const size_t result) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto size = in->size();

            auto out = ColumnUInt8::create(size);

            const auto & in_data = in->getData();
            auto & out_data = out->getData();

            for (const auto i : ext::range(0, size))
                out_data[i] = Impl::execute(in_data[i]);

            columns[result].column = std::move(out);
            return true;
        }

        return false;
    }
};

}
