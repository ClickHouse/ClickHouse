#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperImpl.h>


namespace DB
{
namespace
{

struct NameLower
{
    static constexpr auto name = "lower";
};
using FunctionLower = FunctionStringToString<LowerUpperImpl<'A', 'Z'>, NameLower>;

}

REGISTER_FUNCTION(Lower)
{
    FunctionDocumentation::Description description = R"(
Converts an ASCII string to lowercase.
)";
    FunctionDocumentation::Syntax syntax = "lower(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "A string to convert to lowercase.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a lowercase string from `s`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT lower('CLICKHOUSE')",
        R"(
┌─lower('CLICKHOUSE')─┐
│ clickhouse          │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLower>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("lcase", NameLower::name, FunctionFactory::Case::Insensitive);
}

}
