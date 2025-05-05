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
    Returns the smaller of two numeric values `a` and `b`. The returned value is of type Float64.
    )";
    FunctionDocumentation::Syntax syntax = "min2(a, b)";
    FunctionDocumentation::Argument argument1 = {"a", "First value"};
    FunctionDocumentation::Argument argument2 = {"b", "Second value"};
    FunctionDocumentation::Arguments arguments = {argument1, argument2};
    FunctionDocumentation::ReturnedValue returned_value = "Returns the smaller value of `a` and `b`";
    FunctionDocumentation::Example example1 = {"", "SELECT min2(-1, 2)", "-1"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, categories};
    factory.registerFunction<FunctionMin2>(documentation, FunctionFactory::Case::Insensitive);
}
}
