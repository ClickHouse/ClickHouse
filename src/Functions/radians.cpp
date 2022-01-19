#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

template <typename T>
T radians(T d)
{
    return (d * M_PI) / 180;
}

namespace DB
{
namespace
{
    struct RadiansName
    {
        static constexpr auto name = "radians";
    };
    using FunctionRadians= FunctionMathUnary<UnaryFunctionVectorized<RadiansName, radians>>;

}

void registerFunctionRadians(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRadians>(FunctionFactory::CaseInsensitive);
}

}
