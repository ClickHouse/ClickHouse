#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

REGISTER_FUNCTION(Round)
{
    factory.registerFunction<FunctionRound>("round", {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundBankers>("roundBankers", {}, FunctionFactory::CaseSensitive);
    factory.registerFunction<FunctionRoundHalfUp>(
        {
            R"(Similar to function round,except that in case when given number has equal distance to surrounding numbers, the function rounds away from zero(towards +inf/-inf).)",
            Documentation::Examples{
                {"roundHalfUp", "SELECT roundHalfUp(3.165,2)"}},
            Documentation::Categories{"Rounding"}
        },
        FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionFloor>("floor", {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>("ceil", {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("trunc", {}, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundDown>();


    /// Compatibility aliases.
    factory.registerAlias("ceiling", "ceil", FunctionFactory::CaseInsensitive);
    factory.registerAlias("truncate", "trunc", FunctionFactory::CaseInsensitive);
}

}
