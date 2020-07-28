#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>


namespace DB
{

/// Returns global default value for type of passed argument (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfArgumentType : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfArgumentType";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionDefaultValueOfArgumentType>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const IDataType & type = *block.getByPosition(arguments[0]).type;
        block.getByPosition(result).column = type.createColumnConst(input_rows_count, type.getDefault());
    }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments) const override
    {
        const IDataType & type = *block.getByPosition(arguments[0]).type;
        return type.createColumnConst(1, type.getDefault());
    }
};


void registerFunctionDefaultValueOfArgumentType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDefaultValueOfArgumentType>();
}

}
