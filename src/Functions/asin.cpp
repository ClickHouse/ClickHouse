#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace
{

struct AsinName { static constexpr auto name = "asin"; };
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, asin>>;

}

REGISTER_FUNCTION(Asin)
{
    factory.registerFunction<FunctionAsin>(FunctionFactory::CaseInsensitive);
}

}
