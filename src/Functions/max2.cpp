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
    Returns the bigger of two numeric values `x` and `y`. The returned value is of type Float64.
    )";
    FunctionDocumentation::Syntax syntax = "max2(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "First value"};
    FunctionDocumentation::Argument argument2 = {"y", "Second value"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "Returns the bigger value of `x` and `y`";
    FunctionDocumentation::Examples examples = {{"", "SELECT max2(-1, 2)", "2"}};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionMax2>(documentation, FunctionFactory::Case::Insensitive);
}
}
