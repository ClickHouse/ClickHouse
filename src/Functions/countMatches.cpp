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

REGISTER_FUNCTION(CountMatches)
{
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseSensitive>>();
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseInsensitive>>();
}

}
