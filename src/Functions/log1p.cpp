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

REGISTER_FUNCTION(Log1p)
{
    factory.registerFunction<FunctionLog1p>();
}

}
