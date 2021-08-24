#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/// Returns global default value for type of passed argument (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfArgumentType : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfArgumentType";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionDefaultValueOfArgumentType>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IDataType & type = *arguments[0].type;
        return type.createColumnConst(input_rows_count, type.getDefault());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        const IDataType & type = *arguments[0].type;
        return type.createColumnConst(1, type.getDefault());
    }
};

}

void registerFunctionDefaultValueOfArgumentType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDefaultValueOfArgumentType>();
}

}
