#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TanhName { static constexpr auto name = "tanh"; };

#if USE_FASTOPS

    struct Impl
    {
        static constexpr auto name = TanhName::name;
        static constexpr auto rows_per_iteration = 0;
        static constexpr bool always_returns_float64 = false;

        template <typename T>
        static void execute(const T * src, size_t size, T * dst)
        {
            NFastOps::Tanh<>(src, size, dst);
        }
    };

using FunctionTanh = FunctionMathUnary<Impl>;

#else

double tanh(double x)
{
    return 2 / (1.0 + exp(-2 * x)) - 1;
}

using FunctionTanh = FunctionMathUnary<UnaryFunctionVectorized<TanhName, tanh>>;
#endif

}

void registerFunctionTanh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTanh>(FunctionFactory::CaseInsensitive);
}

}


