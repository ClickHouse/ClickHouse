#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

struct SigmoidName { static constexpr auto name = "sigmoid"; };

#if USE_FASTOPS

namespace
{
    struct Impl
    {
        static constexpr auto name = SigmoidName::name;
        static constexpr auto rows_per_iteration = 0;
        static constexpr bool always_returns_float64 = false;

        template <typename T>
        static void execute(const T * src, size_t size, T * dst)
        {
            if constexpr (std::is_same_v<T, BFloat16>)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function `{}` is not implemented for BFloat16", name);
            }
            else
            {
                NFastOps::Sigmoid<>(src, size, dst);
            }
        }
    };
}

using FunctionSigmoid = FunctionMathUnary<Impl>;

#else

double sigmoid(double x)
{
    return 1.0 / (1.0 + exp(-x));
}

using FunctionSigmoid = FunctionMathUnary<UnaryFunctionVectorized<SigmoidName, sigmoid>>;

#endif

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
