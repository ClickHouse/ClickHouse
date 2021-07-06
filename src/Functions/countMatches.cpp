#include "FunctionFactory.h"
#include "countMatches.h"

namespace
{

struct FunctionCountMatchesCaseSensitive
{
    static constexpr auto name = "countMatches";
    static constexpr bool case_insensitive = false;
};
struct FunctionCountMatchesCaseInsensitive
{
    static constexpr auto name = "countMatchesCaseInsensitive";
    static constexpr bool case_insensitive = true;
};

}

namespace DB
{

void registerFunctionCountMatches(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseSensitive>>(FunctionFactory::CaseSensitive);
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseInsensitive>>(FunctionFactory::CaseSensitive);
}

}
