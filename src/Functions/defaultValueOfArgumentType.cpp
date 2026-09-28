#include <DataTypes/IDataType.h>
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

REGISTER_FUNCTION(DefaultValueOfArgumentType)
{
    FunctionDocumentation::Description description = R"(
Returns the default value for a given data type.
Does not include default values for custom columns set by the user.
)";
    FunctionDocumentation::Syntax syntax = "defaultValueOfArgumentType(expression)";
    FunctionDocumentation::Arguments arguments = {
        {"expression", "Arbitrary type of value or an expression that results in a value of an arbitrary type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0` for numbers, an empty string for strings or `NULL` for Nullable types.", {"UInt8", "String", "NULL"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT defaultValueOfArgumentType(CAST(1 AS Int8));
        )",
        R"(
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
        )"
    },
    {
        "Nullable example",
        R"(
SELECT defaultValueOfArgumentType(CAST(1 AS Nullable(Int8)));
        )",
        R"(
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDefaultValueOfArgumentType>(documentation);
}

}
