#include <Functions/FunctionNumericPredicate.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct IsNaNImpl
{
    static constexpr auto name = "isNaN";
    template <typename T>
    static bool execute(const T t)
    {
        return t != t;
    }
};

using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;

}

REGISTER_FUNCTION(IsNaN)
{
    FunctionDocumentation::Description description = "Returns `1` if the Float32 and Float64 argument is `NaN`, otherwise returns `0`.";
    FunctionDocumentation::Syntax syntax = "isNaN(x)";
    FunctionDocumentation::Arguments arguments =
    {
        {"x", "Argument to evaluate for if it is `NaN`.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"`1` if `NaN`, otherwise `0`"};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT isNaN(NaN)", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionIsNaN>(documentation);
}

}
