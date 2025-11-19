#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace
{

class FunctionToLowCardinality: public IFunction
{
public:
    static constexpr auto name = "toLowCardinality";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToLowCardinality>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->lowCardinality())
            return arguments[0];

        return std::make_shared<DataTypeLowCardinality>(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];

        if (arg.type->lowCardinality())
            return arg.column;

        auto column = res_type->createColumn();
        typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*arg.column, 0, arg.column->size());
        return column;
    }
};

}

REGISTER_FUNCTION(ToLowCardinality)
{
    /// toLowCardinality documentation
    FunctionDocumentation::Description toLowCardinality_description = R"(
Converts the input argument to the [LowCardinality](../data-types/lowcardinality.md) version of same data type.

:::tip
To convert from the `LowCardinality` data type to a regular data type, use the [CAST](#cast) function.
For example: `CAST(x AS String)`.
:::
    )";
    FunctionDocumentation::Syntax toLowCardinality_syntax = "toLowCardinality(expr)";
    FunctionDocumentation::Arguments toLowCardinality_arguments = {
        {"expr", "Expression resulting in one of the supported data types.", {"String", "FixedString", "Date", "DateTime", "(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue toLowCardinality_returned_value = {"Returns the input value converted to the `LowCardinality` data type.", {"LowCardinality"}};
    FunctionDocumentation::Examples toLowCardinality_examples = {
    {
        "Usage example",
        R"(
SELECT toLowCardinality('1')
        )",
        R"(
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toLowCardinality_introduced_in = {18, 12};
    FunctionDocumentation::Category toLowCardinality_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toLowCardinality_documentation = {toLowCardinality_description, toLowCardinality_syntax, toLowCardinality_arguments, toLowCardinality_returned_value, toLowCardinality_examples, toLowCardinality_introduced_in, toLowCardinality_category};

    factory.registerFunction<FunctionToLowCardinality>(toLowCardinality_documentation);
}

}
