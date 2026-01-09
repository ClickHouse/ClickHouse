#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AcosName { static constexpr auto name = "acos"; };
using FunctionAcos = FunctionMathUnary<UnaryFunctionVectorized<AcosName, acos>>;

}

REGISTER_FUNCTION(Acos)
{
    factory.registerFunction<FunctionAcos>({}, FunctionFactory::Case::Insensitive);
}

}
