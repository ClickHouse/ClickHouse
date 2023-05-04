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

REGISTER_FUNCTION(Acosh)
{
    factory.registerFunction<FunctionAcosh>();
}

}
