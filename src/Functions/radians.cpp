#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>


namespace DB
{
namespace
{
    struct RadiansName
    {
        static constexpr auto name = "radians";
    };

    Float64 radians(Float64 d)
    {
        Float64 radians = d * (M_PI / 180);
        return radians;
    }

    using FunctionRadians = FunctionMathUnary<UnaryFunctionVectorized<RadiansName, radians>>;
}

REGISTER_FUNCTION(Radians)
{
    FunctionDocumentation::Description description = R"(
Converts degrees to radians.
)";
    FunctionDocumentation::Syntax syntax = "radians(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input in degrees.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns value in radians", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT radians(180)", "3.141592653589793"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRadians>(documentation, FunctionFactory::Case::Insensitive);
}

}
