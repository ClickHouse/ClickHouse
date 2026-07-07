#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct Exp2Name { static constexpr auto name = "exp2"; };

#if USE_FASTOPS
struct Exp2Fast
{
    static constexpr auto name = Exp2Name::name;
    static void fast(const double * src, size_t size, double * dst) { NFastOps::Exp2<true>(src, size, dst); }
};
struct FunctionExp2
{
    static constexpr auto name = Exp2Name::name;
    static FunctionPtr create(ContextPtr context) { return createGatedMathUnary<Exp2Name, Exp2Fast, exp2>(context); }
};
#else
using FunctionExp2 = FunctionMathUnary<UnaryFunctionVectorized<Exp2Name, exp2>>;
#endif

}

REGISTER_FUNCTION(Exp2)
{
    FunctionDocumentation::Description description = R"(
Returns 2 to the power of the given argument.
)";
    FunctionDocumentation::Syntax syntax = "exp2(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The exponent.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 2^x", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT exp2(3);", "8"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExp2>(documentation);
}

}
