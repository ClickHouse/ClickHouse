#include <Columns/IColumn.h>
#include <Functions/IFunction.h>
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
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionToColumnTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

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
        return DataTypeString().createColumnConst(input_rows_count, arguments[0].column->getName());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        return DataTypeString().createColumnConst(1, arguments[0].type->createColumn()->getName());
    }
};

}

REGISTER_FUNCTION(ToColumnTypeName)
{
    FunctionDocumentation::Description description_toColumnTypeName = R"(
Returns the internal name of the data type that represents the value.
)";
    FunctionDocumentation::Syntax syntax_toColumnTypeName = "toColumnTypeName(value)";
    FunctionDocumentation::Arguments arguments_toColumnTypeName = {
        {"value", "Value for which to return the internal data type name.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toColumnTypeName = {"Returns the internal data type name used to represent the value.", {"String"}};
    FunctionDocumentation::Examples examples_toColumnTypeName = {
    {
        "Usage example",
        R"(
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime));
        )",
        R"(
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toColumnTypeName = {1, 1};
    FunctionDocumentation::Category category_toColumnTypeName = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_toColumnTypeName = {description_toColumnTypeName, syntax_toColumnTypeName, arguments_toColumnTypeName, returned_value_toColumnTypeName, examples_toColumnTypeName, introduced_in_toColumnTypeName, category_toColumnTypeName};

    factory.registerFunction<FunctionToColumnTypeName>(documentation_toColumnTypeName);
}

}
