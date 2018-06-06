#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>

namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundToExp2>();
    factory.registerFunction<FunctionRoundDuration>();
    factory.registerFunction<FunctionRoundAge>();

    factory.registerFunction<FunctionRound>("round", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionFloor>("floor", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>("ceil", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("trunc", FunctionFactory::CaseInsensitive);

    /// Compatibility aliases.
    factory.registerFunction<FunctionCeil>("ceiling", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("truncate", FunctionFactory::CaseInsensitive);
}

}
