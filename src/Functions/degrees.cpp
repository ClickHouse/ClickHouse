#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

template <typename T>
T degrees(T r)
{
    return r * (180 / M_PI);
}

namespace DB
{
namespace
{
    struct DegreesName
    {
        static constexpr auto name = "degrees";
    };
    using FunctionDegrees = FunctionMathUnary<UnaryFunctionVectorized<DegreesName, degrees>>;

}

void registerFunctionDegrees(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDegrees>(FunctionFactory::CaseInsensitive);
}

}
