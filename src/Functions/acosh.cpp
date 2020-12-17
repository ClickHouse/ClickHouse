#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct AcoshName
    {
        static constexpr auto name = "acosh";
    };
    using FunctionAcosh = FunctionMathUnary<UnaryFunctionVectorized<AcoshName, acosh>>;

}

void registerFunctionAcosh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAcosh>();
}

}
