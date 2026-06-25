#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>

namespace DB
{
namespace
{

struct Atan2Name
{
    static constexpr auto name = "atan2";
};
using FunctionAtan2 = FunctionMathBinaryFloat64<BinaryFunctionVectorized<Atan2Name, atan2>>;

}

REGISTER_FUNCTION(Atan2)
{
    FunctionDocumentation::Description description = R"(
Returns the atan2 as the angle in the Euclidean plane, given in radians, between the positive x axis and the ray to the point `(x, y) ≠ (0, 0)`.
)";
    FunctionDocumentation::Syntax syntax = "atan2(y, x)";
    FunctionDocumentation::Arguments arguments = {
        {"y", "y-coordinate of the point through which the ray passes.", {"(U)Int*", "Float*", "Decimal*"}},
        {"x", "x-coordinate of the point through which the ray passes.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the angle `θ` such that `-π < θ ≤ π`, in radians", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT atan2(1, 1)", "0.7853981633974483"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAtan2>(documentation, FunctionFactory::Case::Insensitive);
}

}
