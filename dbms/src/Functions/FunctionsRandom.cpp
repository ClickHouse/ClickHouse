#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

namespace detail
{
    void seed(LinearCongruentialGenerator & generator, intptr_t additional_seed)
    {
        generator.seed(intHash64(randomSeed() ^ intHash64(additional_seed)));
    }
}



void registerFunctionsRandom(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRand>();
    factory.registerFunction<FunctionRand64>();
    factory.registerFunction<FunctionRandConstant>();
}

}
