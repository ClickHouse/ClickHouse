#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct ExpName { static constexpr auto name = "exp"; };

#if USE_FASTOPS

namespace
{
    struct Impl
    {
        static constexpr auto name = ExpName::name;
        static constexpr auto rows_per_iteration = 0;
        static constexpr bool always_returns_float64 = false;

        template <typename T>
        static void execute(const T * src, size_t size, T * dst)
        {
            NFastOps::Exp<true>(src, size, dst);
        }
    };
}

using FunctionExp = FunctionMathUnary<Impl>;

#else
using FunctionExp = FunctionMathUnary<UnaryFunctionVectorized<ExpName, exp>>;
#endif

void registerFunctionExp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExp>(FunctionFactory::CaseInsensitive);
}

}
