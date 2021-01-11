#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRound>("round", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundBankers>("roundBankers", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFloor>("floor", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>("ceil", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("trunc", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundDown>();

    /// Compatibility aliases.
    factory.registerAlias("ceiling", "ceil", FunctionFactory::CaseInsensitive);
    factory.registerAlias("truncate", "trunc", FunctionFactory::CaseInsensitive);
}

}
