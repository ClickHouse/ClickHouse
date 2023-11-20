#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"


namespace DB
{
namespace
{

struct NameMatch
{
    static constexpr auto name = "match";
};

using FunctionMatch = FunctionsStringSearch<MatchImpl<NameMatch, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>>;

}

REGISTER_FUNCTION(Match)
{
    factory.registerFunction<FunctionMatch>();
    factory.registerAlias("REGEXP_MATCHES", NameMatch::name, FunctionFactory::CaseInsensitive);
}

}
