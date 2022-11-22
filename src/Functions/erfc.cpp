#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfcName { static constexpr auto name = "erfc"; };
using FunctionErfc = FunctionMathUnary<UnaryFunctionVectorized<ErfcName, std::erfc>>;

}

void registerFunctionErfc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErfc>();
}

}
