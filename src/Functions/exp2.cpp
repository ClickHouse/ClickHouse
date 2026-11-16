#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#include <bit>
#include <cmath>

namespace DB
{
namespace
{

struct Exp2Name { static constexpr auto name = "exp2"; };

#if USE_FASTOPS
/// `NFastOps::Exp2` is approximate even for an integer argument. The power of two of an integer is exact here, as with
/// `exp2` of libm, so that e.g. `toUInt64(exp2(n))` is `1 << n`: for a normal result its bits are made directly.
/// The kernel reads `src` after it has written `dst`.
void exp2Kernel(const double * src, size_t size, double * dst)
{
    NFastOps::Exp2<true>(src, size, dst);
    for (size_t i = 0; i < size; ++i)
    {
        const double x = src[i];
        if (x == std::trunc(x) && x >= -1022 && x <= 1023)
            dst[i] = std::bit_cast<double>(static_cast<UInt64>(static_cast<Int64>(x) + 1023) << 52);
    }
}
using FunctionExp2 = FunctionMathUnary<VectorizedFloat64Impl<Exp2Name, exp2Kernel, /*ReadsSrcAfterWritingDst=*/ true>>;
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
