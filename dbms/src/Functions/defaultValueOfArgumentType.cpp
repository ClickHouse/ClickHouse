#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


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

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType & type = *block.getByPosition(arguments[0]).type;
        block.getByPosition(result).column = type.createColumnConst(input_rows_count, type.getDefault());
    }
};


void registerFunctionDefaultValueOfArgumentType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDefaultValueOfArgumentType>();
}

}
