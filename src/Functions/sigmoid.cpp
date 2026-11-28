#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/HighwayMath.h>

#include <cmath>

namespace DB
{

namespace
{

struct SigmoidName { static constexpr auto name = "sigmoid"; };

struct Impl
{
    static constexpr auto name = SigmoidName::name;
    static constexpr auto rows_per_iteration = 0;
    static constexpr bool always_returns_float64 = false;

    template <typename T>
    static void execute(const T * src, size_t size, T * dst)
    {
#if USE_HIGHWAY
        HighwayMath::sigmoid(src, size, dst);
#else
        for (size_t i = 0; i < size; ++i)
        {
            /// Unlike 1 / (1 + exp(-x)), does not overflow to 0 for large negative arguments.
            T e = std::exp(-std::abs(src[i]));
            dst[i] = (src[i] < 0 ? e : T(1)) / (T(1) + e);
        }
#endif
    }
};

using FunctionSigmoid = FunctionMathUnary<Impl>;

}

REGISTER_FUNCTION(Sigmoid)
{
    FunctionDocumentation::Description description = R"(
Calculates the sigmoid function: `1 / (1 + exp(-x))`. The sigmoid function maps any real number to the range (0, 1) and is commonly used in machine learning.
    )";
    FunctionDocumentation::Syntax syntax = "sigmoid(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The input value.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the sigmoid of the input value, in the range (0, 1).", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Basic usage", "SELECT sigmoid(0)", "0.5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSigmoid>(documentation);
}

}
