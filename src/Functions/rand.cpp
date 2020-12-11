#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

struct NameRand { static constexpr auto name = "rand"; };
using FunctionRand = FunctionRandom<UInt32, NameRand>;

void registerFunctionRand(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRand>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("rand32", NameRand::name);
}

}

