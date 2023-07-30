#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

using NotLikeImpl = MatchImpl<NameNotLike, MatchTraits::Syntax::Like, MatchTraits::Case::Sensitive, MatchTraits::Result::Negate>;
using FunctionNotLike = FunctionsStringSearch<NotLikeImpl>;

}

REGISTER_FUNCTION(NotLike)
{
    factory.registerFunction<FunctionNotLike>();
}

}
