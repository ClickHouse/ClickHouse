#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>

namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundToExp2>();
    factory.registerFunction<FunctionRoundDuration>();
    factory.registerFunction<FunctionRoundAge>();

    factory.registerFunction("round", FunctionRound::create, FunctionFactory::CaseInsensitive);
    factory.registerFunction("floor", FunctionFloor::create, FunctionFactory::CaseInsensitive);
    factory.registerFunction("ceil", FunctionCeil::create, FunctionFactory::CaseInsensitive);
    factory.registerFunction("trunc", FunctionTrunc::create, FunctionFactory::CaseInsensitive);

    /// Compatibility aliases.
    factory.registerFunction("ceiling", FunctionCeil::create, FunctionFactory::CaseInsensitive);
    factory.registerFunction("truncate", FunctionTrunc::create, FunctionFactory::CaseInsensitive);
}

}
