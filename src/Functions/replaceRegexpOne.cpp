#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceRegexpImpl.h"


namespace DB
{
namespace
{

struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};

using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<NameReplaceRegexpOne, ReplaceRegexpTraits::Replace::First>, NameReplaceRegexpOne>;

}

REGISTER_FUNCTION(ReplaceRegexpOne)
{
    factory.registerFunction<FunctionReplaceRegexpOne>();
}

}
