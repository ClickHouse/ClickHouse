#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class FunctionToTypeName : public IFunction
{
public:

    static constexpr auto name = "toTypeName";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionToTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }


    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, arguments[0].type->getName());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return DataTypeString().createColumnConst(1, arguments[0].type->getName());
    }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
};

}

void registerFunctionToTypeName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToTypeName>();
}

}
