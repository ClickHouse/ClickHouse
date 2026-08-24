#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>

namespace DB
{
namespace
{

struct NameILike
{
    static constexpr auto name = "ilike";
};

using ILikeImpl = MatchImpl<NameILike, MatchTraits::Syntax::Like, MatchTraits::Case::Insensitive, MatchTraits::Result::DontNegate>;
using FunctionILike = FunctionsStringSearch<ILikeImpl>;

}

REGISTER_FUNCTION(ILike)
{
    factory.registerFunction<FunctionILike>();
}

}
