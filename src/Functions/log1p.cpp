#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct Log1pName
    {
        static constexpr auto name = "log1p";
    };
    using FunctionLog1p = FunctionMathUnary<UnaryFunctionVectorized<Log1pName, log1p>>;

}

void registerFunctionLog1p(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLog1p>();
}

}
