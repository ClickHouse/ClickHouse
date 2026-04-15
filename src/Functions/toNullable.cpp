#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Core/ColumnNumbers.h>


namespace DB
{
namespace
{

/// If value is not Nullable or NULL, wraps it to Nullable.
class FunctionToNullable : public IFunction
{
public:
    static constexpr auto name = "toNullable";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionToNullable>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return makeNullable(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        return makeNullable(arguments[0].column);
    }
};

}

REGISTER_FUNCTION(ToNullable)
{
    FunctionDocumentation::Description description = R"(
Converts the provided argument type to `Nullable`.
    )";
    FunctionDocumentation::Syntax syntax = "toNullable(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A value of any non-compound type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value but of `Nullable` type.", {"Nullable(Any)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
SELECT toTypeName(10), toTypeName(toNullable(10));
        )",
         R"(
┌─toTypeName(10)─┬─toTypeName(toNullable(10))─┐
│ UInt8          │ Nullable(UInt8)            │
└────────────────┴────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in{1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToNullable>(documentation);
}

}
