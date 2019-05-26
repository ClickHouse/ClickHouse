#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct ErfcName { static constexpr auto name = "erfc"; };
using FunctionErfc = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfcName, std::erfc>>;

void registerFunctionErfc(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErfc>();
}

}
