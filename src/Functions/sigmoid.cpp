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
    factory.registerFunction<FunctionSigmoid>();
}

}
