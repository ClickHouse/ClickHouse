#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct Log1pName
    {
        static constexpr auto name = "log1p";
    };
    using FunctionLog1p = FunctionMathUnary<UnaryFunctionVectorized<Log1pName, log1p>>;

}

REGISTER_FUNCTION(Log1p)
{
    FunctionDocumentation::Description description = R"(
Calculates log(1+x).
The calculation log1p(x) is more accurate than log(1+x) for small values of `x`.
)";
    FunctionDocumentation::Syntax syntax = "log1p(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Values from the interval: `-1 < x < +∞`.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns values from the interval: -∞ < log1p(x) < +∞", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT log1p(0)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog1p>(documentation);
}

}
