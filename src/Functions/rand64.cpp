#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace
{

struct NameRand64 { static constexpr auto name = "rand64"; };
using FunctionRand64 = FunctionRandom<UInt64, NameRand64>;

}

REGISTER_FUNCTION(Rand64)
{
    factory.registerFunction<FunctionRand64>({}, {.is_deterministic = false, .is_deterministic_in_scope_of_query = false});
}

}


