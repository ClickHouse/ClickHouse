#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathUnary.h>

namespace DB
{
namespace
{
    struct SinhName
    {
        static constexpr auto name = "sinh";
    };
    using FunctionSinh = FunctionMathUnary<UnaryFunctionVectorized<SinhName, sinh>>;

}

REGISTER_FUNCTION(Sinh)
{
    factory.registerFunction<FunctionSinh>();
}

}
