#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperImpl.h>


namespace DB
{
namespace
{

struct NameUpper
{
    static constexpr auto name = "upper";
};
using FunctionUpper = FunctionStringToString<LowerUpperImpl<'a', 'z'>, NameUpper>;

}

REGISTER_FUNCTION(Upper)
{
    FunctionDocumentation::Description description = R"(
Converts the ASCII Latin symbols in a string to uppercase.
)";
    FunctionDocumentation::Syntax syntax = "upper(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "The string to convert to uppercase.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an uppercase string from `s`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT upper('clickhouse')",
        R"(
┌─upper('clickhouse')─┐
│ CLICKHOUSE          │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionUpper>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("ucase", FunctionUpper::name, FunctionFactory::Case::Insensitive);
}

}
