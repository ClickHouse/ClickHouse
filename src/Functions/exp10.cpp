#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <base/preciseExp10.h>

namespace DB
{
namespace
{

struct Exp10Name { static constexpr auto name = "exp10"; };

#if USE_FASTOPS
struct Exp10Fast
{
    static constexpr auto name = Exp10Name::name;
    static void fast(const double * src, size_t size, double * dst) { NFastOps::Exp10<true>(src, size, dst); }
};
struct FunctionExp10
{
    static constexpr auto name = Exp10Name::name;
    static FunctionPtr create(ContextPtr context) { return createGatedMathUnary<Exp10Name, Exp10Fast, preciseExp10>(context); }
};
#else
using FunctionExp10 = FunctionMathUnary<UnaryFunctionVectorized<Exp10Name, preciseExp10>>;
#endif

}

REGISTER_FUNCTION(Exp10)
{
    FunctionDocumentation::Description description = R"(
Returns 10 to the power of the given argument.
)";
    FunctionDocumentation::Syntax syntax = "exp10(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The exponent.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 10^x", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT exp10(2);", "100"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExp10>(documentation);
}

}
