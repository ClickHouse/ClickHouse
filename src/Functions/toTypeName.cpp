#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool print_pretty_type_names;
}

namespace
{

/** toTypeName(x) - get the type name
  * Returns name of IDataType instance (name of data type).
  */
class FunctionToTypeName : public IFunction
{
public:
    explicit FunctionToTypeName(bool print_pretty_type_names_) : print_pretty_type_names(print_pretty_type_names_)
    {
    }

    static constexpr auto name = "toTypeName";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionToTypeName>(context->getSettingsRef()[Setting::print_pretty_type_names]);
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
        return DataTypeString().createColumnConst(input_rows_count, print_pretty_type_names ? arguments[0].type->getPrettyName() : arguments[0].type->getName());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return DataTypeString().createColumnConst(1, print_pretty_type_names ? arguments[0].type->getPrettyName() : arguments[0].type->getName());
    }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

private:
    bool print_pretty_type_names;
};

}

REGISTER_FUNCTION(ToTypeName)
{
    factory.registerFunction<FunctionToTypeName>();
}

}
