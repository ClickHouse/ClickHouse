#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

REGISTER_FUNCTION(Round)
{
    factory.registerFunction<FunctionRound>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundBankers>({}, FunctionFactory::CaseSensitive);
    factory.registerFunction<FunctionFloor>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>({}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundDown>();

    /// Compatibility aliases.
    factory.registerAlias("ceiling", "ceil", FunctionFactory::CaseInsensitive);
    factory.registerAlias("truncate", "trunc", FunctionFactory::CaseInsensitive);
}

}
