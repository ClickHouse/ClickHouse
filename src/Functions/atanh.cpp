#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct AtanhName
    {
        static constexpr auto name = "atanh";
    };
    using FunctionAtanh = FunctionMathUnary<UnaryFunctionVectorized<AtanhName, atanh>>;

}

void registerFunctionAtanh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAtanh>();
}

}
