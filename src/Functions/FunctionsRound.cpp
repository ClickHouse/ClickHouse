#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

REGISTER_FUNCTION(Round)
{
    factory.registerFunction<FunctionRound>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionRoundBankers>({}, FunctionFactory::Case::Sensitive);
    factory.registerFunction<FunctionFloor>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionCeil>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionTrunc>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionRoundDown>();

    /// Compatibility aliases.
    factory.registerAlias("ceiling", "ceil", FunctionFactory::Case::Insensitive);
    factory.registerAlias("truncate", "trunc", FunctionFactory::Case::Insensitive);
}

}
