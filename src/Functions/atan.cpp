#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AtanName { static constexpr auto name = "atan"; };
using FunctionAtan = FunctionMathUnary<UnaryFunctionVectorized<AtanName, atan>>;

}

REGISTER_FUNCTION(Atan)
{
    factory.registerFunction<FunctionAtan>({}, FunctionFactory::Case::Insensitive);
}

}
