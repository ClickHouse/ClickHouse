#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameNotILike
{
    static constexpr auto name = "notILike";
};

using NotILikeImpl = MatchImpl<NameNotILike, MatchTraits::Syntax::Like, MatchTraits::Case::Insensitive, MatchTraits::Result::Negate>;
using FunctionNotILike = FunctionsStringSearch<NotILikeImpl>;

}

REGISTER_FUNCTION(NotILike)
{
    factory.registerFunction<FunctionNotILike>();
}
}
