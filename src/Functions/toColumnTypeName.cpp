#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Returns name of IColumn instance.
class FunctionToColumnTypeName : public IFunction
{
public:
    static constexpr auto name = "toColumnTypeName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionToColumnTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        columns[result].column
            = DataTypeString().createColumnConst(input_rows_count, columns[arguments[0]].column->getName());
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments) const override
    {
        return DataTypeString().createColumnConst(1, columns[arguments[0]].type->createColumn()->getName());
    }
};

}

void registerFunctionToColumnTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToColumnTypeName>();
}

}
