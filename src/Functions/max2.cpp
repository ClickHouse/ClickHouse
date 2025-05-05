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
    Returns the bigger of two numeric values `a` and `b`. The returned value is of type Float64.
    )";
    FunctionDocumentation::Syntax syntax = "max2(a, b)";
    FunctionDocumentation::Argument argument1 = {"a", "First value"};
    FunctionDocumentation::Argument argument2 = {"b", "Second value"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "Returns the bigger value of `a` and `b`";
    FunctionDocumentation::Example example1 = {"", "SELECT max2(-1, 2)", "2"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, categories};
    factory.registerFunction<FunctionMax2>(documentation, FunctionFactory::Case::Insensitive);
}
}
