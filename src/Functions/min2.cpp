#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>


namespace DB
{
namespace
{
    struct Min2Name
    {
        static constexpr auto name = "min2";
    };

    template <typename T>
    T min(T a, T b)
    {
        return a < b ? a : b;
    }

    using FunctionMin2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Min2Name, min>>;
}

REGISTER_FUNCTION(Min2)
{
    FunctionDocumentation::Description description = R"(
    Returns the smaller of two numeric values `x` and `y`.
    )";
    FunctionDocumentation::Syntax syntax = "min2(x, y)";
    FunctionDocumentation::Argument argument1 = {"x", "First value", {"(U)Int8/16/32/64", "Float*", "Decimal"}};
    FunctionDocumentation::Argument argument2 = {"y", "Second value", {"(U)Int8/16/32/64", "Float*", "Decimal"}};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the smaller value of `x` and `y`.", {"Float64"}};
    FunctionDocumentation::Example example1 = {"Usage example", "SELECT min2(-1, 2)", "-1"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMin2>(documentation, FunctionFactory::Case::Insensitive);
}
}
