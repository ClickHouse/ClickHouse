#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace
{

struct NameRand64 { static constexpr auto name = "rand64"; };
using FunctionRand64 = FunctionRandom<UInt64, NameRand64>;

}

void registerFunctionRand64(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRand64>();
}

}


