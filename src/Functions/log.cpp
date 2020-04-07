#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct LogName { static constexpr auto name = "log"; };

#if USE_FASTOPS

namespace
{
    struct Impl
    {
        static constexpr auto name = LogName::name;
        static constexpr auto rows_per_iteration = 0;
        static constexpr bool always_returns_float64 = false;

        template <typename T>
        static void execute(const T * src, size_t size, T * dst)
        {
            NFastOps::Log<true>(src, size, dst);
        }
    };
}

using FunctionLog = FunctionMathUnary<Impl>;

#else
using FunctionLog = FunctionMathUnary<UnaryFunctionVectorized<LogName, log>>;
#endif

void registerFunctionLog(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLog>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("ln", "log", FunctionFactory::CaseInsensitive);
}

}
