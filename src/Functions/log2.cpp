#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

#include <Common/PODArray.h>

#include <bit>
#include <numbers>

namespace DB
{
namespace
{

struct Log2Name { static constexpr auto name = "log2"; };

#if USE_FASTOPS
/// log2(x) = e + log2(m) with x = m * 2^e and m in [1, 2), so the logarithm of a power of two is exact (the logarithm of 1
/// is 0), as with `log2` of libm and as `floor(log2(x))` relies on; it is not negative for m >= 1 either. Zero, subnormal,
/// negative and non-finite numbers are not split. `src` is read whole before `dst` is written, so they may be the same.
void log2Kernel(const double * src, size_t size, double * dst)
{
    static constexpr UInt64 mantissa_mask = (1ULL << 52) - 1;
    PODArray<double> mantissas(size);
    PODArray<double> exponents(size);
    for (size_t i = 0; i < size; ++i)
    {
        const UInt64 bits = std::bit_cast<UInt64>(src[i]);
        const UInt64 biased_exponent = bits >> 52; /// Includes the sign bit, so a negative number is not split.
        const bool split = biased_exponent != 0 && biased_exponent < 0x7FF;
        mantissas[i] = split ? std::bit_cast<double>((bits & mantissa_mask) | (1023ULL << 52)) : src[i];
        exponents[i] = split ? static_cast<double>(static_cast<Int64>(biased_exponent) - 1023) : 0;
    }

    NFastOps::Log<true>(mantissas.data(), size, dst);
    for (size_t i = 0; i < size; ++i)
    {
        const double log2_mantissa = dst[i] * std::numbers::log2e;
        dst[i] = exponents[i] + (mantissas[i] >= 1 ? std::max(log2_mantissa, 0.0) : log2_mantissa);
    }
}
using FunctionLog2 = FunctionMathUnary<VectorizedFloat64Impl<Log2Name, log2Kernel>>;
#else
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, log2>>;
#endif

}

REGISTER_FUNCTION(Log2)
{
    FunctionDocumentation::Description description = R"(
Returns the binary logarithm of the argument.
)";
    FunctionDocumentation::Syntax syntax = "log2(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The number for which to compute the binary logarithm of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the binary logarithm of `x`.", {"Float*"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT log2(8);", "3"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLog2>(documentation, FunctionFactory::Case::Insensitive);
}

}
