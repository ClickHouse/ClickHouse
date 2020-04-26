#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathConstFloat64.h>

namespace DB
{

struct PiImpl
{
    static constexpr auto name = "pi";
    static constexpr double value = 3.1415926535897932384626433832795028841971693;
};

using FunctionPi = FunctionMathConstFloat64<PiImpl>;

void registerFunctionPi(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPi>(FunctionFactory::CaseInsensitive);
}

}
