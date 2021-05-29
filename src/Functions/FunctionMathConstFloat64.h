#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunctionImpl.h>


namespace DB
{

template <typename Impl>
class FunctionMathConstFloat64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMathConstFloat64>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, Impl::value);
    }
};

}
