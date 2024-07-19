#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace
{

struct NameRand { static constexpr auto name = "rand"; };
using FunctionRand = FunctionRandom<UInt32, NameRand>;

}

REGISTER_FUNCTION(Rand)
{
    factory.registerFunction<FunctionRand>({}, {.is_deterministic_in_scope_of_query = false}, FunctionFactory::Case::Insensitive);
    factory.registerAlias("rand32", NameRand::name);
}

}
