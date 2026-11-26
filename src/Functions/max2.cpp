#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB
{
namespace
{
    struct Max2Name
    {
        static constexpr auto name = "max2";
    };

    template <typename T>
    T max(T a, T b)
    {
        return a > b ? a : b;
    }

    using FunctionMax2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Max2Name, max>>;
}

REGISTER_FUNCTION(Max2)
{
    FunctionDocumentation::Description description = R"(
    Returns the bigger of two numeric values `x` and `y`.
    )";
    FunctionDocumentation::Syntax syntax = "max2(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "First value", {"(U)Int8/16/32/64", "Float*", "Decimal"}};
    FunctionDocumentation::Argument argument2 = {"y", "Second value", {"(U)Int8/16/32/64", "Float*", "Decimal"}};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the bigger value of `x` and `y`.", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT max2(-1, 2)", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMax2>(documentation, FunctionFactory::Case::Insensitive);
}
}
