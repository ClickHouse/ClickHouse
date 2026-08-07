#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>


namespace DB
{
namespace
{

struct DegreesName
{
    static constexpr auto name = "degrees";
};

Float64 degrees(Float64 r)
{
    Float64 degrees = r * (180 / M_PI);
    return degrees;
}

using FunctionDegrees = FunctionMathUnary<UnaryFunctionVectorized<DegreesName, degrees>>;

}

REGISTER_FUNCTION(Degrees)
{
    FunctionDocumentation::Description description = R"(
Converts radians to degrees.
)";
    FunctionDocumentation::Syntax syntax = "degrees(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input in radians.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of `x` in degrees.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT degrees(3.141592653589793)", "180"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDegrees>(documentation, FunctionFactory::Case::Insensitive);
}

}
